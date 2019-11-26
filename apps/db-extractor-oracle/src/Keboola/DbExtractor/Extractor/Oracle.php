<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\DbRetryProxy;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorLogger\Logger;
use Symfony\Component\Process\Process;

use Throwable;

class Oracle extends Extractor
{
    private const TABLELESS_CONFIG_FILE = 'tableless.json';
    private const TABLES_CONFIG_FILE = 'getTablesMetadata.json';

    /** @var  array */
    protected $exportConfigFiles;

    /** @var array */
    private $tablesToList = [];

    /** @var bool */
    private $listColumns = true;

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
                $this->exportConfigFiles[$table['name']] = $this->dataDir . '/' . $table['id'] . '.json';
                $this->writeExportConfig($table);
            }
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
        if (!isset($table['query'])) {
            $table['query'] = $this->simpleQuery($table['table'], $table['columns']);
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
            'parameters' => array_merge($parameters, $table)
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
        if (array_key_exists('table', $table) && !array_key_exists('query', $table)) {
            $isAdvancedQuery = false;
            $query = $this->simpleQuery($table['table'], $table['columns']);
        } else {
            $query = $table['query'];
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
        return $output;
    }

    protected function exportTable(string $tableName, bool $advancedQuery): int
    {
        $cmd = [
            'java',
            '-jar',
            '/code/table-exporter.jar',
            'export',
            $this->exportConfigFiles[$tableName],
            var_export($advancedQuery, true),
        ];

        $process = $this->runRetriableCommand($cmd);
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
            '/code/table-exporter.jar',
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
            '/code/table-exporter.jar',
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

    private function quote(string $obj): string
    {
        return "\"{$obj}\"";
    }
}
