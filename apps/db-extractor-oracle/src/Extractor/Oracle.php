<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\DbExtractor\DbRetryProxy;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\TableResultFormat\Table;
use Keboola\DbExtractor\TableResultFormat\TableColumn;
use Keboola\DbExtractorLogger\Logger;
use Symfony\Component\Process\Process;

use Throwable;
use function Keboola\Utils\formatDateTime;

class Oracle extends Extractor
{
    public const INCREMENT_TYPE_NUMERIC = 'numeric';
    public const INCREMENT_TYPE_TIMESTAMP = 'timestamp';
    public const INCREMENT_TYPE_DATE = 'date';
    public const NUMERIC_BASE_TYPES = ['INTEGER', 'NUMERIC', 'FLOAT'];
    private const TABLELESS_CONFIG_FILE = 'tableless.json';
    private const TABLES_CONFIG_FILE = 'getTablesMetadata.json';

    protected array $exportConfigFiles;

    private array $tablesToList = [];

    private bool $listColumns = true;

    public function __construct(array $parameters, array $state = [], ?Logger $logger = null)
    {
        parent::__construct($parameters, $state, $logger);

        // check for special table fetching option
        if (!empty($parameters['tableListFilter'])) {
            if (!empty($parameters['tableListFilter']['tablesToList'])) {
                $this->tablesToList = $parameters['tableListFilter']['tablesToList'];
            }
            if (isset($parameters['tableListFilter']['listColumns'])) {
                $this->listColumns = $parameters['tableListFilter']['listColumns'];
            }
        }

        // setup the export config files for the export tool
        if (array_key_exists('tables', $parameters)) {
            foreach ($parameters['tables'] as $table) {
                $this->writeExportConfig($table);
            }
        } elseif (isset($parameters['id'])) {
            $this->writeExportConfig($parameters);
        }
        $this->writeTablelessConfig();
    }

    private function writeTablelessConfig(): void
    {
        $dbParams = $this->getDbParameters();
        $dbParams['port'] = (string) $dbParams['port'];
        $config = [
            'parameters' => [
                'db' => $dbParams,
                'outputFile' => $this->dataDir . '/' . 'tables.json',
            ],
        ];
        file_put_contents($this->dataDir . '/' . self::TABLELESS_CONFIG_FILE, json_encode($config));
    }

    private function prepareTablesConfig(?array $tables = null): void
    {
        $dbParams = $this->getDbParameters();
        $dbParams['port'] = (string) $dbParams['port'];
        $config = [
            'parameters' => [
                'db' => $dbParams,
                'outputFile' => $this->dataDir . '/' . 'tables.json',
                'tables' => (!empty($tables)) ? $tables : [],
                'includeColumns' => $this->listColumns,
            ],
        ];
        file_put_contents($this->dataDir . '/' . self::TABLES_CONFIG_FILE, json_encode($config));
    }

    private function writeExportConfig(array $table): void
    {
        $this->exportConfigFiles[$table['name']] = $this->dataDir . '/' . $table['id'] . '.json';
        if (!isset($table['query'])) {
            $table['query'] = $this->simplyQueryWithIncrementalAddon(
                $table['table'],
                isset($table['columns']) ? $table['columns'] : []
            );
            unset($table['table']);
        } else {
            $table['query'] = rtrim($table['query'], ' ;');
        }
        $table['outputFile'] = $this->getOutputFilename($table['outputTable']);
        $dbParams = $this->getDbParameters();
        $dbParams['port'] = (string) $dbParams['port'];
        $parameters = array(
            'db' => $dbParams
        );
        $config = array(
            'parameters' => array_merge($table, $parameters)
        );
        file_put_contents($this->exportConfigFiles[$table['name']], json_encode($config));
    }

    public function createConnection(array $params): void
    {
        // not required
    }

    protected function handleDbError(Throwable $e, ?array $table = null, ?int $counter = null): UserException
    {
        $message = '';
        if ($table) {
            $message = sprintf('[%s]: ', $table['name']);
        }
        $message .= sprintf('DB query failed: %s', $e->getMessage());
        if ($counter) {
            $message .= sprintf(' Tried %d times.', $counter);
        }
        return new UserException($message, 0, $e);
    }

    public function export(array $table): array
    {
        $outputTable = $table['outputTable'];
        $csv = $this->createOutputCsv($outputTable);

        $this->logger->info('Exporting to ' . $outputTable);

        $isAdvancedQuery = true;
        $maxValue = null;
        if (array_key_exists('table', $table)) {
            $isAdvancedQuery = false;
            if ($this->incrementalFetching) {
                $maxValue = $this->getMaxOfIncrementalFetchingColumn($table);
            }
        }
        $maxTries = isset($table['retries']) ? (int) $table['retries'] : null;

        /* set backoff initial interval to 1 second */
        $proxy = new DbRetryProxy($this->logger, $maxTries);
        $tableName = $table['name'];
        try {
            $linesWritten = $proxy->call(function () use ($tableName, $isAdvancedQuery) {
                return $this->exportTable($tableName, $isAdvancedQuery);
            });
        } catch (Throwable $e) {
            throw $this->handleDbError($e, $table, $maxTries);
        }
        $rowCount = $linesWritten - 1;
        if ($rowCount > 0) {
            $this->createManifest($table);
        } else {
            @unlink($csv->getPathname());
            $this->logger->warn(
                sprintf(
                    'Query returned empty result. Nothing was imported for table [%s]',
                    $table['name']
                )
            );
        }

        $output = [
            'outputTable'=> $outputTable,
            'rows' => $rowCount,
        ];

        // output state
        if ($maxValue) {
            $output['state']['lastFetchedRow'] = $maxValue;
        }
        return $output;
    }

    public function validateIncrementalFetching(array $table, string $columnName, ?int $limit = null): void
    {
        $table = current($this->getTables([$table]));
        $columns = array_values(array_filter($table['columns'], function ($item) use ($columnName) {
            return $item['name'] === $columnName;
        }));

        try {
            $datatype = new GenericStorage($columns[0]['type']);
            if (in_array($datatype->getBasetype(), self::NUMERIC_BASE_TYPES)) {
                $this->incrementalFetching['column'] = $columnName;
                $this->incrementalFetching['type'] = self::INCREMENT_TYPE_NUMERIC;
            } elseif ($datatype->getBasetype() === 'TIMESTAMP') {
                $this->incrementalFetching['column'] = $columnName;
                $this->incrementalFetching['type'] = self::INCREMENT_TYPE_TIMESTAMP;
            } elseif ($datatype->getBasetype() === 'DATE') {
                $this->incrementalFetching['column'] = $columnName;
                $this->incrementalFetching['type'] = self::INCREMENT_TYPE_DATE;
            } else {
                throw new UserException('invalid incremental fetching column type');
            }
        } catch (InvalidLengthException | UserException $exception) {
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
        $table['id'] .= 'LastRow';
        $table['name'] .= 'LastRow';
        $table['outputTable'] .= 'LastRow';
        $simplyQuery = $this->simplyQueryWithIncrementalAddon(
            $table['table'],
            [$this->incrementalFetching['column']]
        );
        $table['query'] = $this->getLastRowQuery($simplyQuery);
        unset($table['table']);
        $this->writeExportConfig($table);
        $cmd = [
            'java',
            '-jar',
            '/opt/table-exporter.jar',
            'export',
            $this->exportConfigFiles[$table['name']],
            var_export(false, true),
        ];
        $processLastRowValue = new Process($cmd);

        $processLastRowValue
            ->setTimeout(null)
            ->setIdleTimeout(null)
            ->run()
        ;
        return json_decode((string) file_get_contents($this->getOutputFilename($table['outputTable'])));
    }

    protected function exportTable(string $tableName, bool $advancedQuery): int
    {
        $cmd = [
            'java',
            '-jar',
            '/opt/table-exporter.jar',
            'export',
            $this->exportConfigFiles[$tableName],
            var_export($advancedQuery, true),
        ];

        $process = $this->runRetriableCommand($cmd, 'Export process failed');
        // log the process output
        $output = $process->getOutput();
        $this->logger->info($output);

        $fetchedPos = (int) strpos($output, 'Fetched');
        $rowCountStr = substr($output, $fetchedPos, strpos($output, 'rows in') - $fetchedPos);
        $linesWritten = (int) filter_var(
            $rowCountStr,
            FILTER_SANITIZE_NUMBER_INT
        );
        return $linesWritten;
    }

    private function runRetriableCommand(array $cmd, string $errorMessage): Process
    {
        $retryProxy = new DbRetryProxy(
            $this->logger,
            DbRetryProxy::DEFAULT_MAX_TRIES,
            [\ErrorException::class]
        );
        return $retryProxy->call(function () use ($cmd, $errorMessage): Process {
            $process = new Process($cmd);
            $process->setTimeout(null);
            $process->setIdleTimeout(null);
            $process->run();
            if (!$process->isSuccessful()) {
                throw new \ErrorException(sprintf(
                    '%s: %s',
                    $errorMessage,
                    $process->getErrorOutput()
                ));
            }
            return $process;
        });
    }

    public function testConnection(): bool
    {
        $cmd = [
            'java',
            '-jar',
            '/opt/table-exporter.jar',
            'testConnection',
            $this->dataDir . '/' . self::TABLELESS_CONFIG_FILE,
        ];

        $this->runRetriableCommand($cmd, 'Failed connecting to DB');
        return true;
    }

    public function getTables(?array $tables = null): array
    {
        if ($this->tablesToList && !$tables) {
            $tables = $this->tablesToList;
        }

        $this->prepareTablesConfig($tables);
        $cmd = [
            'java',
            '-jar',
            '/opt/table-exporter.jar',
            'getTables',
            $this->dataDir . '/' . self::TABLES_CONFIG_FILE,
        ];

        $this->runRetriableCommand($cmd, 'Error fetching table listing');
        $tableListing = json_decode((string) file_get_contents($this->dataDir . '/tables.json'), true);
        if (json_last_error() !== JSON_ERROR_NONE) {
            throw new ApplicationException(
                'Cannot parse JSON data of table listing - error: ' . json_last_error()
            );
        }
        /** @var Table[] $tableDefs */
        $tableDefs = [];
        foreach ($tableListing as $table) {
            $tableFormat = new Table();
            $tableFormat
                ->setName($table['name'])
                ->setSchema($table['schema']);

            if (isset($table['tablespaceName'])) {
                $tableFormat->setCatalog($table['tablespaceName']);
            }

            if (isset($table['columns'])) {
                foreach ($table['columns'] as $column) {
                    $columnFormat = new TableColumn();
                    $columnFormat
                        ->setName($column['name'])
                        ->setType($column['type'])
                        ->setNullable($column['nullable'])
                        ->setLength($column['length'])
                        ->setOrdinalPosition($column['ordinalPosition'])
                        ->setPrimaryKey($column['primaryKey'])
                        ->setUniqueKey($column['uniqueKey']);

                    $tableFormat->addColumn($columnFormat);
                }
            }
            $tableDefs[] = $tableFormat;
        }
        array_walk($tableDefs, function (Table &$item): void {
            $item = $item->getOutput();
        });
        return $tableListing;
    }

    public function simpleQuery(array $table, array $columns = array()): string
    {
        if (count($columns) > 0) {
            return sprintf(
                'SELECT %s FROM %s.%s',
                implode(
                    ', ',
                    array_map(
                        function ($column) {
                            return $this->quote($column);
                        },
                        $columns
                    )
                ),
                $this->quote($table['schema']),
                $this->quote($table['tableName'])
            );
        } else {
            return sprintf(
                'SELECT * FROM %s.%s',
                $this->quote($table['schema']),
                $this->quote($table['tableName'])
            );
        }
    }

    private function simplyQueryWithIncrementalAddon(
        array $table,
        array $columns = array()
    ): string {
        $incrementalAddonOrder = null;
        $incrementalAddonConditions = [];
        if ($this->incrementalFetching) {
            if (isset($this->incrementalFetching['column']) && isset($this->state['lastFetchedRow'])) {
                if ($this->incrementalFetching['type'] === self::INCREMENT_TYPE_NUMERIC) {
                    $lastFetchedRow = $this->state['lastFetchedRow'];
                } elseif ($this->incrementalFetching['type'] === self::INCREMENT_TYPE_DATE) {
                    $lastFetchedRow = sprintf(
                        'DATE \'%s\'',
                        formatDateTime($this->state['lastFetchedRow'], 'Y-m-d')
                    );
                } else {
                    $lastFetchedRow = $this->quote((string) $this->state['lastFetchedRow']);
                }
                $incrementalAddonConditions[] = sprintf(
                    '%s >= %s',
                    $this->quote($this->incrementalFetching['column']),
                    $lastFetchedRow
                );
            }

            if (isset($this->incrementalFetching['limit'])) {
                $incrementalAddonConditions[] = sprintf(
                    'ROWNUM <= %d',
                    $this->incrementalFetching['limit']
                );
            }
        }

        $query = $this->simpleQuery($table, $columns);

        if (!empty($incrementalAddonConditions)) {
            $query .= ' WHERE ' . implode(' AND ', $incrementalAddonConditions);
        }

        return $query;
    }

    private function getLastRowQuery(string $query): string
    {
        $query = sprintf(
            'SELECT "%s" FROM (%s) ORDER BY "%s" DESC',
            $this->incrementalFetching['column'],
            $query,
            $this->incrementalFetching['column']
        );
        $query = sprintf(
            'SELECT "%s" FROM (%s) WHERE ROWNUM = 1',
            $this->incrementalFetching['column'],
            $query
        );

        return $query;
    }

    private function quote(string $obj): string
    {
        return "\"{$obj}\"";
    }
}
