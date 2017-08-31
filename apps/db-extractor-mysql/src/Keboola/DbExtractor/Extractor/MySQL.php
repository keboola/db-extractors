<?php
/**
 * @package ex-db-mysql
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Exception\UserException;
use Keboola\Temp\Temp;
use Keboola\Datatype\Definition\GenericStorage;
use Symfony\Component\Yaml\Yaml;

class MySQL extends Extractor
{
	/**
	 * @param $sslCa
	 * @param Temp $temp
	 * @return string
	 */
	private function createSSLFile($sslCa, Temp $temp)
	{
		$filename = $temp->createTmpFile('ssl');
		file_put_contents($filename, $sslCa);
		return realpath($filename);
	}

	public function createConnection($params)
	{
		$isSsl = false;

		// convert errors to PDOExceptions
		$options = [
			\PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
		];

		// ssl encryption
		if (!empty($params['ssl']) && !empty($params['ssl']['enabled'])) {
			$ssl = $params['ssl'];

			$temp = new Temp(defined('APP_NAME') ? APP_NAME : 'ex-db-mysql');

			if (!empty($ssl['key'])) {
				$options[\PDO::MYSQL_ATTR_SSL_KEY] = $this->createSSLFile($ssl['key'], $temp);
				$isSsl = true;
			}
			if (!empty($ssl['cert'])) {
				$options[\PDO::MYSQL_ATTR_SSL_CERT] = $this->createSSLFile($ssl['cert'], $temp);
				$isSsl = true;
			}
			if (!empty($ssl['ca'])) {
				$options[\PDO::MYSQL_ATTR_SSL_CA] = $this->createSSLFile($ssl['ca'], $temp);
				$isSsl = true;
			}
			if (!empty($ssl['cipher'])) {
				$options[\PDO::MYSQL_ATTR_SSL_CIPHER] = $ssl['cipher'];
			}
		}

		foreach (['host', 'database', 'user', 'password'] as $r) {
			if (!array_key_exists($r, $params)) {
				throw new UserException(sprintf("Parameter %s is missing.", $r));
			}
		}

		$port = !empty($params['port']) ? $params['port'] : '3306';
		$dsn = sprintf(
			"mysql:host=%s;port=%s;dbname=%s;charset=utf8",
			$params['host'],
			$port,
			$params['database']
		);

		$this->logger->info("Connecting to DSN '" . $dsn . "' " . ($isSsl ? 'Using SSL' : ''));

		$pdo = new \PDO($dsn, $params['user'], $params['password'], $options);
		$pdo->setAttribute(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
		$pdo->exec("SET NAMES utf8;");

		if ($isSsl) {
			$status = $pdo->query("SHOW STATUS LIKE 'Ssl_cipher';")->fetch(\PDO::FETCH_ASSOC);

			if (empty($status['Value'])) {
				throw new UserException(sprintf("Connection is not encrypted"));
			} else {
				$this->logger->info("Using SSL cipher: " . $status['Value']);
			}
		}

		return $pdo;
	}

	public function getConnection()
	{
		return $this->db;
	}

	public function testConnection()
	{
		$this->db->query('SELECT NOW();')->execute();
	}

    public function getTables(array $tables = null)
    {
        $sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES 
                                  WHERE TABLE_SCHEMA != 'performance_schema' 
                                  AND TABLE_SCHEMA != 'mysql'
                                  AND TABLE_SCHEMA != 'information_schema'";

        if (!is_null($tables) && count($tables) > 0) {
            $sql .= sprintf(
                " AND TABLE_NAME IN (%s)",
                implode(',', array_map(function ($table) {
                    return $this->db->quote($table);
                }, $tables))
            );
        }

        $res = $this->db->query($sql);
        $arr = $res->fetchAll(\PDO::FETCH_ASSOC);

        $output = [];
        foreach ($arr as $table) {
            $output[] = $this->describeTable($table);
        }
        return $output;
    }

    protected function describeTable(array $table)
    {
        $tabledef = [
            'name' => $table['TABLE_NAME'],
            'schema' => (isset($table['TABLE_SCHEMA'])) ? $table['TABLE_SCHEMA'] : null,
            'type' => (isset($table['TABLE_TYPE'])) ? $table['TABLE_TYPE'] : null,
            'rowCount' => (isset($table['TABLE_ROWS'])) ? $table['TABLE_ROWS'] : null
        ];

        $sql = sprintf("SELECT c.*, 
                    CONSTRAINT_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME, REFERENCED_TABLE_SCHEMA
                    FROM INFORMATION_SCHEMA.COLUMNS as c 
                    LEFT OUTER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE as kcu
                    ON c.TABLE_NAME = kcu.TABLE_NAME AND c.COLUMN_NAME = kcu.COLUMN_NAME
                    WHERE c.TABLE_NAME = %s", $this->db->quote($table['TABLE_NAME']));

        $res = $this->db->query($sql);
        $columns = [];

        $rows = $res->fetchAll(\PDO::FETCH_ASSOC);
        foreach ($rows as $i => $column) {
            $length = ($column['CHARACTER_MAXIMUM_LENGTH']) ? $column['CHARACTER_MAXIMUM_LENGTH'] : null;
            if (is_null($length) && !is_null($column['NUMERIC_PRECISION'])) {
                if ($column['NUMERIC_SCALE'] > 0) {
                    $length = $column['NUMERIC_PRECISION'] . "," . $column['NUMERIC_SCALE'];
                } else {
                    $length = $column['NUMERIC_PRECISION'];
                }
            }
            $columns[] = [
                "name" => $column['COLUMN_NAME'],
                "type" => $column['DATA_TYPE'],
                "primaryKey" => ($column['COLUMN_KEY'] === "PRI") ? true : false,
                "length" => $length,
                "nullable" => ($column['IS_NULLABLE'] === "NO") ? false : true,
                "default" => $column['COLUMN_DEFAULT'],
                "ordinalPosition" => $column['ORDINAL_POSITION']
            ];

            if (!is_null($column['CONSTRAINT_NAME']) ) {
                $columns[$i]['constraintName'] = $column['CONSTRAINT_NAME'];
            }
            if (!is_null($column['REFERENCED_TABLE_NAME'])) {
                $columns[$i]['foreignKeyRefSchema'] = $column['REFERENCED_TABLE_SCHEMA'];
                $columns[$i]['foreignKeyRefTable'] = $column['REFERENCED_TABLE_NAME'];
                $columns[$i]['foreignKeyRefColumn'] = $column['REFERENCED_COLUMN_NAME'];
            }
        }
        $tabledef['columns'] = $columns;
        return $tabledef;
    }

    protected function createManifest($table)
    {
        $outFilename = $this->dataDir . '/out/tables/' . $table['outputTable'] . '.csv.manifest';

        $manifestData = [
            'destination' => $table['outputTable'],
            'incremental' => $table['incremental']
        ];

        if (!empty($table['primaryKey'])) {
            $manifestData['primary_key'] = $table['primaryKey'];
        }

        if (isset($table['table']) && !is_null($table['table']) && !empty($table['columns'])) {
            $tableDetails = $this->getTables([$table['table']])[0];
            $columnMetadata = [];
            foreach ($tableDetails['columns'] as $column) {
                if (!in_array($column['name'], $table['columns'])) {
                    continue;
                }
                $datatypeKeys = ['type', 'length', 'nullable', 'default', 'format'];
                $datatype = new GenericStorage(
                    $column['type'],
                    array_intersect_key($column, array_flip($datatypeKeys))
                );
                $columnMetadata[$column['name']] = $datatype->toMetadata();
                $nonDatatypeKeys = array_diff_key($column, array_flip($datatypeKeys));
                foreach ($nonDatatypeKeys as $key => $value) {
                    if ($key !== 'name') {
                        $columnMetadata[$column['name']][] = [
                            'key' => "KBC." . $key,
                            'value'=> $value
                        ];
                    }
                }
            }
            unset($tableDetails['columns']);
            foreach ($tableDetails as $key => $value) {
                $manifestData['metadata'][] = [
                    "key" => "KBC." . $key,
                    "value" => $value
                ];
            }
            $manifestData['column_metadata'] = $columnMetadata;
        }
        return file_put_contents($outFilename, Yaml::dump($manifestData));
    }

    public function simpleQuery($table, $columns = array())
    {
        if (count($columns) > 0) {
            return sprintf("SELECT %s FROM %s",
                implode(', ', array_map(function ($column) {
                    return $this->quote($column);
                }, $columns)),
                $this->quote($table)
            );
        } else {
            return sprintf("SELECT * FROM %s", $this->quote($table));
        }
    }

    private function quote($obj) {
        return "`{$obj}`";
    }
}
