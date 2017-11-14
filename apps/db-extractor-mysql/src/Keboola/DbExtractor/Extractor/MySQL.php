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
    protected $database;

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

        foreach (['host', 'user', 'password'] as $r) {
            if (!array_key_exists($r, $params)) {
                throw new UserException(sprintf("Parameter %s is missing.", $r));
            }
        }

        $port = !empty($params['port']) ? $params['port'] : '3306';

        $dsn = sprintf(
            "mysql:host=%s;port=%s;charset=utf8",
            $params['host'],
            $port
        );

        if (isset($params['database'])) {
            $dsn = sprintf(
                "mysql:host=%s;port=%s;dbname=%s;charset=utf8",
                $params['host'],
                $port,
                $params['database']
            );
            $this->database = $params['database'];
        }

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

        $sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES as c";

        $whereClause = " WHERE c.TABLE_SCHEMA != 'performance_schema' 
                          AND c.TABLE_SCHEMA != 'mysql'
                          AND c.TABLE_SCHEMA != 'information_schema'";

        if ($this->database) {
            $whereClause = sprintf(" WHERE c.TABLE_SCHEMA = %s", $this->db->quote($this->database));
        }

        if (!is_null($tables) && count($tables) > 0) {
            $whereClause .= sprintf(
                " AND c.TABLE_NAME IN (%s) AND c.TABLE_SCHEMA IN (%s)",
                implode(',', array_map(function ($table) {
                    return $this->db->quote($table['tableName']);
                }, $tables)),
                implode(',', array_map(function ($table) {
                    return $this->db->quote($table['schema']);
                }, $tables))
            );
        }

        $sql .= $whereClause;

        $sql .= " ORDER BY TABLE_SCHEMA, TABLE_NAME";

        $res = $this->db->query($sql);
        $arr = $res->fetchAll(\PDO::FETCH_ASSOC);

        if (count($arr) === 0) {
            return [];
        }

        $tableNameArray = [];
        $tableDefs = [];
        foreach ($arr as $table) {
            $tableNameArray[] = $table['TABLE_NAME'];
            $tableDefs[$table['TABLE_SCHEMA'] . '.' . $table['TABLE_NAME']] = [
                'name' => $table['TABLE_NAME'],
                'schema' => (isset($table['TABLE_SCHEMA'])) ? $table['TABLE_SCHEMA'] : '',
                'type' => (isset($table['TABLE_TYPE'])) ? $table['TABLE_TYPE'] : '',
                'rowCount' => (isset($table['TABLE_ROWS'])) ? $table['TABLE_ROWS'] : ''
            ];
        }

        if (!is_null($tables) && count($tables) > 0) {
            $sql = "SELECT c.*, 
                    CONSTRAINT_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME, REFERENCED_TABLE_SCHEMA
                    FROM INFORMATION_SCHEMA.COLUMNS as c 
                    LEFT OUTER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE as kcu
                    ON c.TABLE_NAME = kcu.TABLE_NAME AND c.COLUMN_NAME = kcu.COLUMN_NAME";
        } else {
            $sql = "SELECT c.*
                    FROM INFORMATION_SCHEMA.COLUMNS as c";
        }

        $sql .= $whereClause;

        $sql .= " ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, ORDINAL_POSITION";

        $res = $this->db->query($sql);
        $rows = $res->fetchAll(\PDO::FETCH_ASSOC);
        foreach ($rows as $i => $column) {
            $curTable = $column['TABLE_SCHEMA'] . '.' . $column['TABLE_NAME'];
            $length = ($column['CHARACTER_MAXIMUM_LENGTH']) ? $column['CHARACTER_MAXIMUM_LENGTH'] : null;
            if (is_null($length) && !is_null($column['NUMERIC_PRECISION'])) {
                if ($column['NUMERIC_SCALE'] > 0) {
                    $length = $column['NUMERIC_PRECISION'] . "," . $column['NUMERIC_SCALE'];
                } else {
                    $length = $column['NUMERIC_PRECISION'];
                }
            }
            $curColumn = [
                "name" => $column['COLUMN_NAME'],
                "type" => $column['DATA_TYPE'],
                "primaryKey" => ($column['COLUMN_KEY'] === "PRI") ? true : false,
                "length" => $length,
                "nullable" => ($column['IS_NULLABLE'] === "NO") ? false : true,
                "default" => $column['COLUMN_DEFAULT'],
                "ordinalPosition" => $column['ORDINAL_POSITION']
            ];

            if (array_key_exists('CONSTRAINT_NAME', $column) && !is_null($column['CONSTRAINT_NAME'])) {
                $curColumn['constraintName'] = $column['CONSTRAINT_NAME'];
            }
            if (array_key_exists('REFERENCED_TABLE_NAME', $column) && !is_null($column['REFERENCED_TABLE_NAME'])) {
                $curColumn['foreignKeyRefSchema'] = $column['REFERENCED_TABLE_SCHEMA'];
                $curColumn['foreignKeyRefTable'] = $column['REFERENCED_TABLE_NAME'];
                $curColumn['foreignKeyRefColumn'] = $column['REFERENCED_COLUMN_NAME'];
            }
            $tableDefs[$curTable]['columns'][$column['ORDINAL_POSITION'] - 1] = $curColumn;
        }
        return array_values($tableDefs);
    }

    protected function describeTable(array $table)
    {
        // Deprecated
        return null;
    }

    public function simpleQuery(array $table, array $columns = array())
    {
        if (count($columns) > 0) {
            return sprintf(
                "SELECT %s FROM %s.%s",
                implode(', ', array_map(function ($column) {
                    return $this->quote($column);
                }, $columns)),
                $this->quote($table['schema']),
                $this->quote($table['tableName'])
            );
        } else {
            return sprintf(
                "SELECT * FROM %s.%s",
                $this->quote($table['schema']),
                $this->quote($table['tableName'])
            );
        }
    }

    private function quote($obj)
    {
        return "`{$obj}`";
    }
}
