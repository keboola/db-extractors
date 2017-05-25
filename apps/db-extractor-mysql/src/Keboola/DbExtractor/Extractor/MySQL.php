<?php
/**
 * @package ex-db-mysql
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Exception\UserException;
use Keboola\Temp\Temp;

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

    public function listTables()
    {
        $tables = [];
        $res = $this->db->query("SHOW TABLES");
        while ($table = $res->fetch(\PDO::FETCH_NUM)) {
            $tables[] = $table[0];
        }
        return $tables;
    }

    public function describeTable($tableName)
    {
        $res = $this->db->query(sprintf("SELECT 
                    COLUMN_NAME, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, 
                    CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, COLUMN_KEY
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = %s", $this->db->quote($tableName)));
        $columns = [];
        while ($column = $res->fetch(\PDO::FETCH_ASSOC)) {
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
                "primary" => ($column['COLUMN_KEY'] === "PRI") ? true : false,
                "length" => $length,
                "nullable" => ($column['IS_NULLABLE'] === "NO") ? false : true,
                "default" => $column['COLUMN_DEFAULT']
            ];
        }
        return $columns;
    }
}
