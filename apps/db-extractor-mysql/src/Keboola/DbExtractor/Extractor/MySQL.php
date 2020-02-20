<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Datatype\Definition\MySQL as MysqlDatatype;
use Keboola\DbExtractor\DbRetryProxy;
use Keboola\DbExtractor\Exception\DeadConnectionException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\TableResultFormat\ForeignKey;
use Keboola\DbExtractor\TableResultFormat\Table;
use Keboola\DbExtractor\TableResultFormat\TableColumn;
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
        return (string) realpath((string) $filename);
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

            $temp = new Temp(getenv('APP_NAME') ? (string) getenv('APP_NAME') : 'ex-db-mysql');

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
                throw new UserException(sprintf('Parameter %s is missing.', $r));
            }
        }

        $port = !empty($params['port']) ? $params['port'] : '3306';

        $dsn = sprintf(
            'mysql:host=%s;port=%s;charset=utf8',
            $params['host'],
            $port
        );

        if (isset($params['database'])) {
            $dsn = sprintf(
                'mysql:host=%s;port=%s;dbname=%s;charset=utf8',
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
            $previous = $e->getPrevious();
            if ($previous !== null) {
                $checkCnMismatch($previous);
            }
            throw $e;
        }
        $pdo->setAttribute(PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
        $pdo->exec('SET NAMES utf8;');

        if ($isSsl) {
            $status = $pdo->query("SHOW STATUS LIKE 'Ssl_cipher';")->fetch(PDO::FETCH_ASSOC);

            if (empty($status['Value'])) {
                throw new UserException(sprintf('Connection is not encrypted'));
            } else {
                $this->logger->info('Using SSL cipher: ' . $status['Value']);
            }
        }

        if ($isCompression) {
            $status = $pdo->query("SHOW SESSION STATUS LIKE 'Compression';")->fetch(PDO::FETCH_ASSOC);

            if (empty($status['Value']) || $status['Value'] !== 'ON') {
                throw new UserException(sprintf('Network communication is not compressed'));
            } else {
                $this->logger->info('Using network communication compression');
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

        $sql = 'SELECT * FROM INFORMATION_SCHEMA.TABLES as c';

        $whereClause = " WHERE c.TABLE_SCHEMA != 'performance_schema' 
                          AND c.TABLE_SCHEMA != 'mysql'
                          AND c.TABLE_SCHEMA != 'information_schema'
                          AND c.TABLE_SCHEMA != 'sys'";

        if ($this->database) {
            $whereClause = sprintf(' WHERE c.TABLE_SCHEMA = %s', $this->db->quote($this->database));
        }

        if (!is_null($tables) && count($tables) > 0) {
            $whereClause .= sprintf(
                ' AND c.TABLE_NAME IN (%s) AND c.TABLE_SCHEMA IN (%s)',
                implode(',', array_map(function ($table) {
                    return $this->db->quote($table['tableName']);
                }, $tables)),
                implode(',', array_map(function ($table) {
                    return $this->db->quote($table['schema']);
                }, $tables))
            );
        }

        $sql .= $whereClause;
        $arr = $this->runRetriableQuery($sql);
        if (count($arr) === 0) {
            return [];
        }

        /** @var Table[] $tableDefs */
        $tableDefs = [];
        $autoIncrements = [];
        foreach ($arr as $table) {
            $curTable = $table['TABLE_SCHEMA'] . '.' . $table['TABLE_NAME'];
            $tableFormat = new Table();
            $tableFormat
                ->setName($table['TABLE_NAME'])
                ->setSchema((isset($table['TABLE_SCHEMA'])) ? $table['TABLE_SCHEMA'] : '')
                ->setType((isset($table['TABLE_TYPE'])) ? $table['TABLE_TYPE'] : '')
                ->setRowCount((isset($table['TABLE_ROWS'])) ? (int) $table['TABLE_ROWS'] : null);

            if (!empty($table['TABLE_COMMENT'])) {
                $tableFormat->setDescription($table['TABLE_COMMENT']);
            }

            if (!empty($table['AUTO_INCREMENT'])) {
                $autoIncrements[$curTable] = (int) $table['AUTO_INCREMENT'];
            }

            $tableDefs[$curTable] = $tableFormat;
        }
        ksort($tableDefs);

        // add additional info
        $foreignKeys = [];
        if (!is_null($tables) && count($tables) > 0) {
            $additionalSql = 'SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, 
                    CONSTRAINT_NAME, REFERENCED_TABLE_NAME, LOWER(REFERENCED_COLUMN_NAME) as REFERENCED_COLUMN_NAME, 
                    REFERENCED_TABLE_SCHEMA FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS c ';

            $rows = $this->runRetriableQuery($additionalSql . $whereClause);
            foreach ($rows as $column) {
                $foreignKey = new ForeignKey();
                if (array_key_exists('CONSTRAINT_NAME', $column) && !is_null($column['CONSTRAINT_NAME'])) {
                    $foreignKey->setName($column['CONSTRAINT_NAME']);
                }
                if (!array_key_exists('REFERENCED_TABLE_NAME', $column) || is_null($column['REFERENCED_TABLE_NAME'])) {
                    continue;
                }
                $foreignKey
                    ->setRefSchema($column['REFERENCED_TABLE_SCHEMA'])
                    ->setRefTable($column['REFERENCED_TABLE_NAME'])
                    ->setRefColumn($column['REFERENCED_COLUMN_NAME']);
                $curTableName = $column['TABLE_SCHEMA'] . '.' . $column['TABLE_NAME'] . '.' . $column['COLUMN_NAME'];
                $foreignKeys[$curTableName] = $foreignKey;
            }
        }

        $sql = 'SELECT c.* FROM INFORMATION_SCHEMA.COLUMNS as c';
        $sql .= $whereClause;
        $rows = $this->runRetriableQuery($sql);
        foreach ($rows as $i => $column) {
            $curTable = $column['TABLE_SCHEMA'] . '.' . $column['TABLE_NAME'];
            $curColumn = $curTable . '.' . $column['COLUMN_NAME'];
            $length = ($column['CHARACTER_MAXIMUM_LENGTH']) ? $column['CHARACTER_MAXIMUM_LENGTH'] : null;
            if (is_null($length) && !is_null($column['NUMERIC_PRECISION'])) {
                if ($column['NUMERIC_SCALE'] > 0) {
                    $length = $column['NUMERIC_PRECISION'] . ',' . $column['NUMERIC_SCALE'];
                } else {
                    $length = $column['NUMERIC_PRECISION'];
                }
            }

            $columnFormat = new TableColumn();
            $columnFormat
                ->setName($column['COLUMN_NAME'])
                ->setType($column['DATA_TYPE'])
                ->setPrimaryKey(($column['COLUMN_KEY'] === 'PRI') ? true : false)
                ->setLength($length)
                ->setNullable(($column['IS_NULLABLE'] === 'NO') ? false : true)
                ->setDefault($column['COLUMN_DEFAULT'])
                ->setOrdinalPosition((int) $column['ORDINAL_POSITION']);

            if ($column['COLUMN_COMMENT']) {
                $columnFormat->setDescription($column['COLUMN_COMMENT']);
            }

            if ($column['EXTRA']) {
                if ($column['EXTRA'] === 'auto_increment') {
                    $columnFormat->setAutoIncrement(true);
                    if (isset($autoIncrements[$curTable])) {
                        $columnFormat->setAutoIncrementValue($autoIncrements[$curTable]);
                    }
                }
            }
            if (isset($foreignKeys[$curColumn])) {
                $columnFormat->setForeignKey($foreignKeys[$curColumn]);
            }
            $tableDefs[$curTable]->addColumn($columnFormat);
        }
        array_walk($tableDefs, function (Table &$item): void {
            $item = $item->getOutput();
            if (isset($item['columns'])) {
                usort($item['columns'], function ($a, $b) {
                    return (int) ($a['ordinalPosition'] > $b['ordinalPosition']);
                });
            }
        });

        return array_values($tableDefs);
    }

    public function validateIncrementalFetching(array $table, string $columnName, ?int $limit = null): void
    {
        $query = sprintf(
            'SELECT * FROM INFORMATION_SCHEMA.COLUMNS as cols 
                        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s',
            $this->db->quote($table['schema']),
            $this->db->quote($table['tableName']),
            $this->db->quote($columnName)
        );
        $columns = $this->runRetriableQuery($query);
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

    public function getMaxOfIncrementalFetchingColumn(array $table): ?string
    {
        $sql = 'SELECT MAX(%s) as %s FROM %s.%s';
        $fullsql = sprintf(
            $sql,
            $this->quote($this->incrementalFetching['column']),
            $this->quote($this->incrementalFetching['column']),
            $this->quote($table['schema']),
            $this->quote($table['tableName'])
        );
        $result = $this->runRetriableQuery($fullsql);
        if (count($result) > 0) {
            return $result[0][$this->incrementalFetching['column']];
        }
        return null;
    }

    public function simpleQuery(array $table, array $columns = []): string
    {
        $incrementalAddon = null;
        if ($this->incrementalFetching && isset($this->incrementalFetching['column'])) {
            if (isset($this->state['lastFetchedRow'])) {
                $incrementalAddon = sprintf(
                    ' WHERE %s >= %s',
                    $this->quote($this->incrementalFetching['column']),
                    $this->db->quote((string) $this->state['lastFetchedRow'])
                );
            }
            $incrementalAddon .= sprintf(' ORDER BY %s', $this->quote($this->incrementalFetching['column']));
        }
        if (count($columns) > 0) {
            $query = sprintf(
                'SELECT %s FROM %s.%s',
                implode(', ', array_map(function ($column): string {
                    return $this->quote($column);
                }, $columns)),
                $this->quote($table['schema']),
                $this->quote($table['tableName'])
            );
        } else {
            $query = sprintf(
                'SELECT * FROM %s.%s',
                $this->quote($table['schema']),
                $this->quote($table['tableName'])
            );
        }

        if ($incrementalAddon) {
            $query .= $incrementalAddon;
        }
        if (isset($this->incrementalFetching['limit'])) {
            $query .= sprintf(
                ' LIMIT %d',
                $this->incrementalFetching['limit']
            );
        }
        return $query;
    }

    private function quote(string $obj): string
    {
        return "`{$obj}`";
    }

    private function runRetriableQuery(string $query, array $values = []): array
    {
        $retryProxy = new DbRetryProxy($this->logger);
        return $retryProxy->call(function () use ($query, $values) {
            try {
                $stmt = $this->db->prepare($query);
                $stmt->execute($values);
                return $stmt->fetchAll(\PDO::FETCH_ASSOC);
            } catch (\Throwable $exception) {
                $this->tryReconnect();
                throw $exception;
            }
        });
    }

    private function tryReconnect(): void
    {
        try {
            $this->isAlive();
        } catch (DeadConnectionException $deadConnectionException) {
            $reconnectionRetryProxy = new DbRetryProxy($this->logger, self::DEFAULT_MAX_TRIES, null, 1000);
            try {
                $this->db = $reconnectionRetryProxy->call(function () {
                    return $this->createConnection($this->getDbParameters());
                });
            } catch (\Throwable $reconnectException) {
                throw new UserException(
                    'Unable to reconnect to the database: ' . $reconnectException->getMessage(),
                    $reconnectException->getCode(),
                    $reconnectException
                );
            }
        }
    }
}
