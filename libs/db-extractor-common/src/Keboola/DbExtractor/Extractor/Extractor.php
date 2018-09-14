<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Csv\CsvFile;
use Keboola\Csv\Exception as CsvException;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\DeadConnectionException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Logger;
use Keboola\DbExtractor\RetryProxy;
use Keboola\SSHTunnel\SSH;
use Keboola\SSHTunnel\SSHException;
use Nette\Utils;

use Throwable;
use PDO;
use PDOStatement;

abstract class Extractor
{
    public const DEFAULT_MAX_TRIES = 5;

    /** @var PDO|mixed */
    protected $db;

    /** @var  array */
    protected $state;

    /** @var  array|null with keys type (autoIncrement or timestamp), column, and limit */
    protected $incrementalFetching;

    /** @var Logger */
    protected $logger;

    /** @var string */
    protected $dataDir;

    /** @var array */
    private $dbParameters;

    public function __construct(array $parameters, array $state = [], ?Logger $logger = null)
    {
        if ($logger) {
            $this->logger = $logger;
        }
        $this->dataDir = $parameters['data_dir'];
        $this->state = $state;

        if (isset($parameters['db']['ssh']['enabled']) && $parameters['db']['ssh']['enabled']) {
            $parameters['db'] = $this->createSshTunnel($parameters['db']);
        }
        $this->dbParameters = $parameters['db'];

        try {
            $this->db = $this->createConnection($this->dbParameters);
        } catch (Throwable $e) {
            if (strstr(strtolower($e->getMessage()), 'could not find driver')) {
                throw new ApplicationException("Missing driver: " . $e->getMessage());
            }
            throw new UserException("Error connecting to DB: " . $e->getMessage(), 0, $e);
        }
        if (isset($parameters['incrementalFetchingColumn']) && $parameters['incrementalFetchingColumn'] !== "") {
            $this->validateIncrementalFetching(
                $parameters['table'],
                $parameters['incrementalFetchingColumn'],
                isset($parameters['incrementalFetchingLimit']) ? $parameters['incrementalFetchingLimit'] : null
            );
        }
    }

    public function createSshTunnel(array $dbConfig): array
    {
        $sshConfig = $dbConfig['ssh'];
        // check params
        foreach (['keys', 'sshHost'] as $k) {
            if (empty($sshConfig[$k])) {
                throw new UserException(sprintf("Parameter '%s' is missing.", $k));
            }
        }
        
        $sshConfig['remoteHost'] = $dbConfig['host'];
        $sshConfig['remotePort'] = $dbConfig['port'];

        if (empty($sshConfig['user'])) {
            $sshConfig['user'] = $dbConfig['user'];
        }
        if (empty($sshConfig['localPort'])) {
            $sshConfig['localPort'] = 33006;
        }
        if (empty($sshConfig['sshPort'])) {
            $sshConfig['sshPort'] = 22;
        }
        $sshConfig['privateKey'] = isset($sshConfig['keys']['#private'])
            ?$sshConfig['keys']['#private']
            :$sshConfig['keys']['private'];
        $tunnelParams = array_intersect_key(
            $sshConfig,
            array_flip(
                [
                'user', 'sshHost', 'sshPort', 'localPort', 'remoteHost', 'remotePort', 'privateKey',
                ]
            )
        );
        $this->logger->info("Creating SSH tunnel to '" . $tunnelParams['sshHost'] . "'");
        $proxy = new RetryProxy(
            $this->logger,
            RetryProxy::DEFAULT_MAX_TRIES,
            RetryProxy::DEFAULT_BACKOFF_INTERVAL,
            ['SSHException', 'Exception']
        );
        try {
            $proxy->call(function () use ($tunnelParams):void {
                $ssh = new SSH();
                $ssh->openTunnel($tunnelParams);
            });
        } catch (SSHException $e) {
            throw new UserException($e->getMessage(), 0, $e);
        }

        $dbConfig['host'] = '127.0.0.1';
        $dbConfig['port'] = $sshConfig['localPort'];

        return $dbConfig;
    }

    /**
     * @param array $params
     * @return PDO|mixed
     */
    abstract public function createConnection(array $params);

    /**
     * @return void|mixed
     */
    abstract public function testConnection();

    /**
     * @param array|null $tables - an optional array of tables with tableName and schema properties
     * @return mixed
     */
    abstract public function getTables(?array $tables = null): array;

    abstract public function simpleQuery(array $table, array $columns = array()): string;

    /**
     * @param array $table
     * @param string $columnName
     * @param int|null $limit
     * @throws UserException
     */
    public function validateIncrementalFetching(array $table, string $columnName, ?int $limit = null): void
    {
        throw new UserException('Incremental Fetching is not supported by this extractor.');
    }

    public function export(array $table): array
    {
        $outputTable = $table['outputTable'];

        $this->logger->info("Exporting to " . $outputTable);

        $isAdvancedQuery = true;
        if (array_key_exists('table', $table) && !array_key_exists('query', $table)) {
            $isAdvancedQuery = false;
            $query = $this->simpleQuery($table['table'], $table['columns']);
        } else {
            $query = $table['query'];
        }
        $maxTries = isset($table['retries']) ? (int) $table['retries'] : self::DEFAULT_MAX_TRIES;

        // this will retry on CsvException
        $proxy = new RetryProxy(
            $this->logger,
            $maxTries,
            RetryProxy::DEFAULT_BACKOFF_INTERVAL,
            ['Keboola\DbExtractor\Exception\DeadConnectionException']
        );
        try {
            $result = $proxy->call(function () use ($query, $maxTries, $outputTable, $isAdvancedQuery) {
                /** @var PDOStatement $stmt */
                $stmt = $this->executeQuery($query, $maxTries);
                $csv = $this->createOutputCsv($outputTable);
                $result = $this->writeToCsv($stmt, $csv, $isAdvancedQuery);
                $this->isAlive();
                return $result;
            });
        } catch (CsvException $e) {
            throw new ApplicationException("Failed writing CSV File: " . $e->getMessage(), $e->getCode(), $e);
        } catch (Throwable $e) {
            throw $this->handleDbError($e, $table, $maxTries);
        }
        if ($result['rows'] > 0) {
            $this->createManifest($table);
        } else {
            $this->logger->warn(
                sprintf(
                    "Query returned empty result. Nothing was imported for table [%s]",
                    $table['name']
                )
            );
        }

        $output = [
            "outputTable"=> $outputTable,
            "rows" => $result['rows'],
        ];
        // output state
        if (!empty($result['lastFetchedRow'])) {
            $output["state"]['lastFetchedRow'] = $result['lastFetchedRow'];
        }
        return $output;
    }

    protected function handleDbError(Throwable $e, ?array $table = null, ?int $counter = null): UserException
    {
        $message = "";
        if ($table) {
            $message = sprintf("[%s]: ", $table['name']);
        }
        $message .= sprintf('DB query failed: %s', $e->getMessage());
        if ($counter) {
            $message .= sprintf(' Tried %d times.', $counter);
        }
        return new UserException($message, 0, $e);
    }

    protected function isAlive(): void
    {
        // needs to be implemented in extractors.
        // implementations should throw an DeadConnectionException if the connection is dead
    }

    protected function executeQuery(string $query, ?int $maxTries): PDOStatement
    {
        $proxy = new RetryProxy($this->logger, $maxTries);
        $stmt = $proxy->call(function () use ($query) {
            try {
                /** @var \PDOStatement $stmt */
                $stmt = $this->db->prepare($query);
                $stmt->execute();
                return $stmt;
            } catch (Throwable $e) {
                try {
                    $this->db = $this->createConnection($this->dbParameters);
                } catch (Throwable $e) {
                };
                throw $e;
            }
        });
        return $stmt;
    }

    /**
     * @param PDOStatement $stmt
     * @param CsvFile $csv
     * @param boolean $includeHeader
     * @return array ['rows', 'lastFetchedRow']
     */
    protected function writeToCsv(PDOStatement $stmt, CsvFile $csv, bool $includeHeader = true): array
    {
        $output = [];

        $resultRow = $stmt->fetch(PDO::FETCH_ASSOC);

        if (is_array($resultRow) && !empty($resultRow)) {
            // write header and first line
            if ($includeHeader) {
                $csv->writeRow(array_keys($resultRow));
            }
            $csv->writeRow($resultRow);

            // write the rest
            $numRows = 1;
            $lastRow = $resultRow;

            while ($resultRow = $stmt->fetch(PDO::FETCH_ASSOC)) {
                $csv->writeRow($resultRow);
                $lastRow = $resultRow;
                $numRows++;
            }

            if (isset($this->incrementalFetching['column'])) {
                if (!array_key_exists($this->incrementalFetching['column'], $lastRow)) {
                    throw new UserException(
                        sprintf(
                            "The specified incremental fetching column %s not found in the table",
                            $this->incrementalFetching['column']
                        )
                    );
                }
                $output['lastFetchedRow'] = $lastRow[$this->incrementalFetching['column']];
            }
            $output['rows'] = $numRows;
            return $output;
        }
        // no rows found.  If incremental fetching is turned on, we need to preserve the last state
        if ($this->incrementalFetching['column'] && isset($this->state['lastFetchedRow'])) {
            $output = $this->state;
        }
        $output['rows'] = 0;
        return $output;
    }

    protected function createOutputCsv(string $outputTable): CsvFile
    {
        $outTablesDir = $this->dataDir . '/out/tables';
        if (!is_dir($outTablesDir)) {
            mkdir($outTablesDir, 0777, true);
        }
        return new CsvFile($this->getOutputFilename($outputTable));
    }

    /**
     * @param array $table
     * @return bool|int
     */
    protected function createManifest(array $table)
    {
        $outFilename = $this->getOutputFilename($table['outputTable']) . '.manifest';

        $manifestData = [
            'destination' => $table['outputTable'],
            'incremental' => $table['incremental'],
        ];

        if (!empty($table['primaryKey'])) {
            $manifestData['primary_key'] = $table['primaryKey'];
        }

        $manifestColumns = [];

        if (isset($table['table']) && !is_null($table['table'])) {
            $tables = $this->getTables([$table['table']]);
            if (count($tables) > 0) {
                $tableDetails = $tables[0];
                $columnMetadata = [];
                $sanitizedPks = [];
                $iterColumns = $table['columns'];
                if (count($iterColumns) === 0) {
                    $iterColumns = array_map(function ($column) {
                        return $column['name'];
                    }, $tableDetails['columns']);
                }
                foreach ($iterColumns as $ind => $columnName) {
                    $column = null;
                    foreach ($tableDetails['columns'] as $detailColumn) {
                        if ($detailColumn['name'] === $columnName) {
                            $column = $detailColumn;
                        }
                    }
                    if (!$column) {
                        throw new UserException(
                            sprintf("The given column '%s' was not found in the table.", $columnName)
                        );
                    }
                    // use sanitized name for primary key if available
                    if (in_array($column['name'], $table['primaryKey']) && array_key_exists('sanitizedName', $column)) {
                        $sanitizedPks[] = $column['sanitizedName'];
                    }
                    $columnName = $column['name'];
                    if (array_key_exists('sanitizedName', $column)) {
                        $columnName = $column['sanitizedName'];
                    }
                    $columnMetadata[$columnName] = $this->getColumnMetadata($column);
                    $manifestColumns[] = $columnName;
                }
                $manifestData['metadata'] = $this->getTableLevelMetadata($tableDetails);

                $manifestData['column_metadata'] = $columnMetadata;
                $manifestData['columns'] = $manifestColumns;
                if (!empty($sanitizedPks)) {
                    $manifestData['primary_key'] = $sanitizedPks;
                }
            }
        }
        return file_put_contents($outFilename, json_encode($manifestData));
    }

    private function getColumnMetadata(array $column): array
    {
        $datatypeKeys = ['type', 'length', 'nullable', 'default', 'format'];
        $datatype = new GenericStorage(
            $column['type'],
            array_intersect_key($column, array_flip($datatypeKeys))
        );
        $columnMetadata = $datatype->toMetadata();
        $nonDatatypeKeys = array_diff_key($column, array_flip($datatypeKeys));
        foreach ($nonDatatypeKeys as $key => $value) {
            if ($key === 'name') {
                $columnMetadata[] = [
                    'key' => "KBC.sourceName",
                    'value' => $value,
                ];
            } else {
                $columnMetadata[] = [
                    'key' => "KBC." . $key,
                    'value' => $value,
                ];
            }
        }
        return $columnMetadata;
    }

    private function getTableLevelMetadata(array $tableDetails): array
    {
        $metadata = [];
        foreach ($tableDetails as $key => $value) {
            if ($key === 'columns') {
                continue;
            }
            $metadata[] = [
                "key" => "KBC." . $key,
                "value" => $value,
            ];
        }
        return $metadata;
    }

    protected function getOutputFilename(string $outputTableName): string
    {
        $sanitizedTablename = Utils\Strings::webalize($outputTableName, '._');
        return $this->dataDir . '/out/tables/' . $sanitizedTablename . '.csv';
    }

    protected function getDbParameters(): array
    {
        return $this->dbParameters;
    }
}
