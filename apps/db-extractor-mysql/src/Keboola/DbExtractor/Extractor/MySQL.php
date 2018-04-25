<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\Temp\Temp;

class MySQL extends Extractor
{
    const TYPE_AUTO_INCREMENT = 'autoIncrement';
    const TYPE_TIMESTAMP = 'timestamp';

    protected $database;

    /**
     * @param $sslCa
     * @param Temp $temp
     * @return string
     */
    private function createSSLFile($sslCa, Temp $temp)
    {
        $filename = $temp->createTmpFile('ssl');
        file_put_contents((string) $filename, $sslCa);
        return realpath((string) $filename);
    }

    public function createConnection($params)
    {
        $isSsl = false;
        $isCompression = !empty($params['networkCompression']) ? true :false;

        $options = [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION, // convert errors to PDOExceptions
            \PDO::MYSQL_ATTR_COMPRESS => $isCompression, // network compression
        ];

        // ssl encryption
        if (!empty($params['ssl']) && !empty($params['ssl']['enabled'])) {
            $ssl = $params['ssl'];

            $temp = new Temp(getenv('APP_NAME') ? getenv('APP_NAME') : 'ex-db-mysql');

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

        try {
            $pdo = new \PDO($dsn, $params['user'], $params['password'], $options);
        } catch (\PDOException $e) {
            $checkCnMismatch = function (\Exception $exception) {
                if (strpos($exception->getMessage(), 'did not match expected CN') !== false) {
                    throw new UserException($exception->getMessage());
                }
            };
            $checkCnMismatch($e);
            if (($previous = $e->getPrevious()) !== null) {
                $checkCnMismatch($previous);
            }
            throw $e;
        }
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

        if ($isCompression) {
            $status = $pdo->query("SHOW SESSION STATUS LIKE 'Compression';")->fetch(\PDO::FETCH_ASSOC);

            if (empty($status['Value']) || $status['Value'] !== 'ON') {
                throw new UserException(sprintf("Network communication is not compressed"));
            } else {
                $this->logger->info("Using network communication compression");
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

    public function export(array $table)
    {
        // if database set make sure the database and selected table schema match
        if (isset($table['table']) && $this->database && $this->database !== $table['table']['schema']) {
            throw new UserException(sprintf(
                'Invalid Configuration in "%s".  The table schema "%s" is different from the connection database "%s"',
                $table['name'],
                $table['table']['schema'],
                $this->database
            ));
        }

        return parent::export($table);
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
            $curTable = $table['TABLE_SCHEMA'] . '.' . $table['TABLE_NAME'];
            $tableDefs[$curTable] = [
                'name' => $table['TABLE_NAME'],
                'schema' => (isset($table['TABLE_SCHEMA'])) ? $table['TABLE_SCHEMA'] : '',
                'type' => (isset($table['TABLE_TYPE'])) ? $table['TABLE_TYPE'] : '',
                'rowCount' => (isset($table['TABLE_ROWS'])) ? $table['TABLE_ROWS'] : '',
                'description' => (isset($table['TABLE_COMMENT'])) ? $table['TABLE_COMMENT'] : ''
            ];
            if ($table["AUTO_INCREMENT"]) {
                $tableDefs[$curTable]['autoIncrement'] = $table['AUTO_INCREMENT'];
            }
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
                "sanitizedName" => \Keboola\Utils\sanitizeColumnName($column['COLUMN_NAME']),
                "type" => $column['DATA_TYPE'],
                "primaryKey" => ($column['COLUMN_KEY'] === "PRI") ? true : false,
                "length" => $length,
                "nullable" => ($column['IS_NULLABLE'] === "NO") ? false : true,
                "default" => $column['COLUMN_DEFAULT'],
                "ordinalPosition" => $column['ORDINAL_POSITION'],
            ];

            if ($column['COLUMN_COMMENT']) {
                $curColumn['description'] = $column['COLUMN_COMMENT'];
            }

            if (array_key_exists('CONSTRAINT_NAME', $column) && !is_null($column['CONSTRAINT_NAME'])) {
                $curColumn['constraintName'] = $column['CONSTRAINT_NAME'];
            }
            if (array_key_exists('REFERENCED_TABLE_NAME', $column) && !is_null($column['REFERENCED_TABLE_NAME'])) {
                $curColumn['foreignKeyRefSchema'] = $column['REFERENCED_TABLE_SCHEMA'];
                $curColumn['foreignKeyRefTable'] = $column['REFERENCED_TABLE_NAME'];
                $curColumn['foreignKeyRefColumn'] = $column['REFERENCED_COLUMN_NAME'];
            }
            if ($column['EXTRA']) {
                $curColumn["extra"] = $column["EXTRA"];
                if ($column['EXTRA'] === 'auto_increment') {
                    $curColumn['autoIncrement'] = $tableDefs[$curTable]['autoIncrement'];
                }
                if ($column['EXTRA'] === 'on update CURRENT_TIMESTAMP' && $column['COLUMN_DEFAULT'] === 'CURRENT_TIMESTAMP') {
                    $tableDefs[$curTable]['timestampUpdateColumn'] = $column['COLUMN_NAME'];
                }
            }
            $tableDefs[$curTable]['columns'][$column['ORDINAL_POSITION'] - 1] = $curColumn;
        }
        return array_values($tableDefs);
    }

    /**
     * @throws UserException
     */
    public function validateIncrementalFetching(array $table, string $columnName, int $limit = null)
    {
        $res = $this->db->query(
            sprintf(
                'SELECT * FROM INFORMATION_SCHEMA.COLUMNS as cols 
                            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s',
                $this->db->quote($table['schema']),
                $this->db->quote($table['tableName']),
                $this->db->quote($columnName)
            )
        );
        $columns = $res->fetchAll();
        if (count($columns) === 0) {
            throw new UserException(
                sprintf(
                    'Column [%s] specified for incremental fetching was not found in the table',
                    $columnName
                )
            );
        }
        if ($columns[0]['EXTRA'] === 'auto_increment') {
            $this->incrementalFetching['column'] = $columnName;
            $this->incrementalFetching['type'] = self::TYPE_AUTO_INCREMENT;
        } else if ($columns[0]['DATA_TYPE'] === 'timestamp') {
            $this->incrementalFetching['column'] = $columnName;
            $this->incrementalFetching['type'] = self::TYPE_TIMESTAMP;
        } else {
            throw new UserException(
                sprintf(
                    'Column [%s] specified for incremental fetching is not an auto increment column or an auto update timestamp',
                    $columnName
                )
            );
        }
        if ($limit) {
            $this->incrementalFetching['limit'] = $limit;
        }
    }

    public function simpleQuery(array $table, array $columns = array()): string
    {
        $incrementalAddon = null;
        if ($this->incrementalFetching && isset($this->state['lastFetchedRow'])) {
            if ($this->incrementalFetching['type'] === self::TYPE_AUTO_INCREMENT) {
                $incrementalAddon = sprintf(
                    ' %s > %d',
                    $this->quote($this->incrementalFetching['column']),
                    (int) $this->state['lastFetchedRow']
                );
            } else if ($this->incrementalFetching['type'] === self::TYPE_TIMESTAMP) {
                $incrementalAddon = sprintf(
                    " %s > '%s'",
                    $this->quote($this->incrementalFetching['column']),
                    $this->state['lastFetchedRow']
                );
            } else {
                throw new ApplicationException(
                    sprintf('Unknown incremental fetching column type %s', $this->incrementalFetching['type'])
                );
            }
        }
        if (count($columns) > 0) {
            $query = sprintf(
                "SELECT %s FROM %s.%s",
                implode(', ', array_map(function ($column) {
                    return $this->quote($column);
                }, $columns)),
                $this->quote($table['schema']),
                $this->quote($table['tableName'])
            );
        } else {
            $query = sprintf(
                "SELECT * FROM %s.%s",
                $this->quote($table['schema']),
                $this->quote($table['tableName'])
            );
        }

        if ($incrementalAddon) {
            $query .= sprintf(
                " WHERE %s ORDER BY %s",
                $incrementalAddon,
                $this->quote($this->incrementalFetching['column'])
            );
        }
        if (isset($this->incrementalFetching['limit'])) {
            $query .= sprintf(
                " LIMIT %d",
                $this->incrementalFetching['limit']
            );
        }
        return $query;
    }

    private function quote($obj)
    {
        return "`{$obj}`";
    }
}
