<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Datatype\Definition\MySQL as MysqlDatatype;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\Temp\Temp;
use PDO;
use PDOException;

class MySQL extends Extractor
{
    public const INCREMENT_TYPE_NUMERIC = 'numeric';
    public const INCREMENT_TYPE_TIMESTAMP = 'timestamp';
    public const NUMERIC_BASE_TYPES = ['INTEGER', 'NUMERIC', 'FLOAT'];

    /** @var  string -- database name from connection parameters */
    protected $database;

    private function createSSLFile(string $sslCa, Temp $temp): string
    {
        $filename = $temp->createTmpFile('ssl');
        file_put_contents((string) $filename, $sslCa);
        return realpath((string) $filename);
    }

    public function createConnection(array $params): PDO
    {
        $isSsl = false;
        $isCompression = !empty($params['networkCompression']) ? true :false;

        $options = [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION, // convert errors to PDOExceptions
            PDO::MYSQL_ATTR_COMPRESS => $isCompression, // network compression
        ];

        // ssl encryption
        if (!empty($params['ssl']) && !empty($params['ssl']['enabled'])) {
            $ssl = $params['ssl'];

            $temp = new Temp(getenv('APP_NAME') ? getenv('APP_NAME') : 'ex-db-mysql');

            if (!empty($ssl['key'])) {
                $options[PDO::MYSQL_ATTR_SSL_KEY] = $this->createSSLFile($ssl['key'], $temp);
                $isSsl = true;
            }
            if (!empty($ssl['cert'])) {
                $options[PDO::MYSQL_ATTR_SSL_CERT] = $this->createSSLFile($ssl['cert'], $temp);
                $isSsl = true;
            }
            if (!empty($ssl['ca'])) {
                $options[PDO::MYSQL_ATTR_SSL_CA] = $this->createSSLFile($ssl['ca'], $temp);
                $isSsl = true;
            }
            if (!empty($ssl['cipher'])) {
                $options[PDO::MYSQL_ATTR_SSL_CIPHER] = $ssl['cipher'];
            }
            if (isset($ssl['verifyServerCert']) && $ssl['verifyServerCert'] === false) {
                $options[PDO::MYSQL_ATTR_SSL_VERIFY_SERVER_CERT] = false;
            }
        }

        foreach (['host', 'user', '#password'] as $r) {
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
            $pdo = new PDO($dsn, $params['user'], $params['#password'], $options);
        } catch (PDOException $e) {
            $checkCnMismatch = function (\Throwable $exception): void {
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
        $pdo->setAttribute(PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
        $pdo->exec("SET NAMES utf8;");

        if ($isSsl) {
            $status = $pdo->query("SHOW STATUS LIKE 'Ssl_cipher';")->fetch(PDO::FETCH_ASSOC);

            if (empty($status['Value'])) {
                throw new UserException(sprintf("Connection is not encrypted"));
            } else {
                $this->logger->info("Using SSL cipher: " . $status['Value']);
            }
        }

        if ($isCompression) {
            $status = $pdo->query("SHOW SESSION STATUS LIKE 'Compression';")->fetch(PDO::FETCH_ASSOC);

            if (empty($status['Value']) || $status['Value'] !== 'ON') {
                throw new UserException(sprintf("Network communication is not compressed"));
            } else {
                $this->logger->info("Using network communication compression");
            }
        }

        return $pdo;
    }

    public function getConnection(): PDO
    {
        return $this->db;
    }

    public function testConnection(): void
    {
        $this->db->query('SELECT NOW();')->execute();
    }

    public function export(array $table): array
    {
        // if database set make sure the database and selected table schema match
        if (isset($table['table']) && $this->database && $this->database !== $table['table']['schema']) {
            throw new UserException(sprintf(
                'Invalid Configuration [%s].  The table schema "%s" is different from the connection database "%s"',
                $table['table']['tableName'],
                $table['table']['schema'],
                $this->database
            ));
        }

        return parent::export($table);
    }

    public function getTables(?array $tables = null): array
    {

        $sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES as c";

        $whereClause = " WHERE c.TABLE_SCHEMA != 'performance_schema' 
                          AND c.TABLE_SCHEMA != 'mysql'
                          AND c.TABLE_SCHEMA != 'information_schema'
                          AND c.TABLE_SCHEMA != 'sys'";

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

        $res = $this->db->query($sql);
        $arr = $res->fetchAll(PDO::FETCH_ASSOC);
        if (count($arr) === 0) {
            return [];
        }

        $tableDefs = [];
        foreach ($arr as $table) {
            $curTable = $table['TABLE_SCHEMA'] . '.' . $table['TABLE_NAME'];
            $tableDefs[$curTable] = [
                'name' => $table['TABLE_NAME'],
                'schema' => (isset($table['TABLE_SCHEMA'])) ? $table['TABLE_SCHEMA'] : '',
                'type' => (isset($table['TABLE_TYPE'])) ? $table['TABLE_TYPE'] : '',
                'rowCount' => (isset($table['TABLE_ROWS'])) ? $table['TABLE_ROWS'] : '',
            ];
            if ($table["TABLE_COMMENT"]) {
                $tableDefs[$curTable]['description'] = $table['TABLE_COMMENT'];
            }
            if ($table["AUTO_INCREMENT"]) {
                $tableDefs[$curTable]['autoIncrement'] = $table['AUTO_INCREMENT'];
            }
        }

        ksort($tableDefs);

        $sql = "SELECT c.* FROM INFORMATION_SCHEMA.COLUMNS as c";
        $sql .= $whereClause;

        $res = $this->db->query($sql);
        $rows = $res->fetchAll(PDO::FETCH_ASSOC);

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

            if ($column['EXTRA']) {
                if ($column['EXTRA'] === 'auto_increment' && isset($tableDefs[$curTable]['autoIncrement'])) {
                    $curColumn['autoIncrement'] = $tableDefs[$curTable]['autoIncrement'];
                }
            }
            $tableDefs[$curTable]['columns'][$column['ORDINAL_POSITION'] - 1] = $curColumn;
            ksort($tableDefs[$curTable]['columns']);
        }

        // add additional info
        if (!is_null($tables) && count($tables) > 0) {
            $additionalSql = "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, 
                    CONSTRAINT_NAME, REFERENCED_TABLE_NAME, LOWER(REFERENCED_COLUMN_NAME) as REFERENCED_COLUMN_NAME, 
                    REFERENCED_TABLE_SCHEMA FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS c ";

            $res = $this->db->query($additionalSql . $whereClause);
            $rows = $res->fetchAll(PDO::FETCH_ASSOC);
            foreach ($rows as $column) {
                $curColumn = [];
                if (array_key_exists('CONSTRAINT_NAME', $column) && !is_null($column['CONSTRAINT_NAME'])) {
                    $curColumn['constraintName'] = $column['CONSTRAINT_NAME'];
                }
                if (array_key_exists('REFERENCED_TABLE_NAME', $column) && !is_null($column['REFERENCED_TABLE_NAME'])) {
                    $curColumn['foreignKeyRefSchema'] = $column['REFERENCED_TABLE_SCHEMA'];
                    $curColumn['foreignKeyRefTable'] = $column['REFERENCED_TABLE_NAME'];
                    $curColumn['foreignKeyRefColumn'] = $column['REFERENCED_COLUMN_NAME'];
                }
                if (count($curColumn) > 0) {
                    $curTableName = $column['TABLE_SCHEMA'] . '.' . $column['TABLE_NAME'];
                    $filteredColumns = array_filter(
                        $tableDefs[$curTableName]['columns'],
                        function ($existingCol) use ($column) {
                            return $existingCol['name'] === $column['COLUMN_NAME'];
                        }
                    );
                    if (count($filteredColumns) === 0) {
                        throw new ApplicationException(
                            sprintf(
                                "This should never happen: Could not find reference column [%s] in table definition",
                                $column['COLUMN_NAME']
                            )
                        );
                    }
                    $existingColumnKey = array_keys($filteredColumns)[0];
                    foreach ($curColumn as $key => $value) {
                        $tableDefs[$curTableName]['columns'][$existingColumnKey][$key] = $value;
                    }
                }
            }
        }
        return array_values($tableDefs);
    }

    public function validateIncrementalFetching(array $table, string $columnName, ?int $limit = null): void
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

        try {
            $datatype = new MysqlDatatype($columns[0]['DATA_TYPE']);
            if (in_array($datatype->getBasetype(), self::NUMERIC_BASE_TYPES)) {
                $this->incrementalFetching['column'] = $columnName;
                $this->incrementalFetching['type'] = self::INCREMENT_TYPE_NUMERIC;
            } else if ($datatype->getBasetype() === 'TIMESTAMP') {
                $this->incrementalFetching['column'] = $columnName;
                $this->incrementalFetching['type'] = self::INCREMENT_TYPE_TIMESTAMP;
            } else {
                throw new UserException('invalid incremental fetching column type');
            }
        } catch (\Keboola\Datatype\Definition\Exception\InvalidLengthException | UserException $exception) {
            throw new UserException(
                sprintf(
                    'Column [%s] specified for incremental fetching is not a numeric or timestamp type column',
                    $columnName
                )
            );
        }

        if ($limit) {
            $this->incrementalFetching['limit'] = $limit;
        }
    }

    public function simpleQuery(array $table, array $columns = []): string
    {
        $incrementalAddon = null;
        if ($this->incrementalFetching && isset($this->incrementalFetching['column'])) {
            if (isset($this->state['lastFetchedRow'])) {
                $incrementalAddon = sprintf(
                    " WHERE %s >= %s",
                    $this->quote($this->incrementalFetching['column']),
                    $this->db->quote((string) $this->state['lastFetchedRow'])
                );
            }
            $incrementalAddon .= sprintf(" ORDER BY %s", $this->quote($this->incrementalFetching['column']));
        }
        if (count($columns) > 0) {
            $query = sprintf(
                "SELECT %s FROM %s.%s",
                implode(', ', array_map(function ($column): string {
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
            $query .= $incrementalAddon;
        }
        if (isset($this->incrementalFetching['limit'])) {
            $query .= sprintf(
                " LIMIT %d",
                $this->incrementalFetching['limit']
            );
        }
        return $query;
    }

    private function quote(string $obj): string
    {
        return "`{$obj}`";
    }
}
