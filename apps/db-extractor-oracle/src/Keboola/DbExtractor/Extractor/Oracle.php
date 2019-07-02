<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Logger;
use Keboola\DbExtractor\RetryProxy;
use Keboola\Utils;
use Symfony\Component\Process\Process;

use Throwable;

class Oracle extends Extractor
{
    const TABLELESS_CONFIG_FILE = "tableless.json";
    const TABLES_CONFIG_FILE = "getTablesMetadata.json";

    protected $db;

    /** @var  array */
    protected $dbParams;

    /** @var  array */
    protected $exportConfigFiles;

    public function __construct(array $parameters, array $state = [], ?Logger $logger = null)
    {
        $this->dbParams = $parameters['db'];
        parent::__construct($parameters, $state, $logger);
        // setup the export config files for the export tool
        foreach ($parameters['tables'] as $table) {
            $this->exportConfigFiles[$table['name']] = $this->dataDir . "/" . $table['id'] . ".json";
            $this->writeExportConfig($this->dbParams, $table);
        }
        $this->writeTablelessConfig($this->dbParams);
    }

    private function writeTablelessConfig(array $dbParams): void
    {
        $dbParams['port'] = (string) $dbParams['port'];
        $config = [
            'parameters' => [
                'db' => $dbParams,
                'outputFile' => $this->dataDir . "/" . 'tables.json'
            ]
        ];
        file_put_contents($this->dataDir . "/" . self::TABLELESS_CONFIG_FILE, json_encode($config));
    }

    private function prepareTablesConfig(array $tables = null): void
    {
        $config = [
            'parameters' => [
                'db' => $this->dbParams,
                'outputFile' => $this->dataDir . "/" . 'tables.json',
                'tables' => (!empty($tables)) ? $tables : []
            ]
        ];
        file_put_contents($this->dataDir . "/" . self::TABLES_CONFIG_FILE, json_encode($config));
    }

    private function writeExportConfig(array $dbParams, array $table): void
    {
        if (!isset($table['query'])) {
            $table['query'] = $this->simpleQuery($table['table'], $table['columns']);
        } else {
            $table['query'] = rtrim($table['query'], " ;");
        }
        $table['outputFile'] = $this->getOutputFilename($table['outputTable']);
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

    public function createSshTunnel(array $dbConfig): array
    {
        $this->dbParams = parent::createSshTunnel($dbConfig);
        return $this->dbParams;
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

    public function export(array $table): array
    {
        $outputTable = $table['outputTable'];
        $csv = $this->createOutputCsv($outputTable);

        $this->logger->info("Exporting to " . $outputTable);

        $isAdvancedQuery = true;
        if (array_key_exists('table', $table) && !array_key_exists('query', $table)) {
            $isAdvancedQuery = false;
            $query = $this->simpleQuery($table['table'], $table['columns']);
        } else {
            $query = $table['query'];
        }
        $maxTries = isset($table['retries']) ? (int) $table['retries'] : null;

        $proxy = new RetryProxy($this->logger, $maxTries);
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
                    "Query returned empty result. Nothing was imported for table [%s]",
                    $table['name']
                )
            );
        }

        $output = [
            "outputTable"=> $outputTable,
            "rows" => $rowCount,
        ];
        return $output;
    }

    protected function exportTable(string $tableName, bool $advancedQuery): int
    {
        $cmd = [
            'java',
            '-jar',
            '/code/oracle/table-exporter.jar',
            'export',
            $this->exportConfigFiles[$tableName],
            var_export($advancedQuery, true),
        ];

        $process = new Process($cmd);
        $process->setTimeout(null);
        $process->setIdleTimeout(null);
        $process->run();

        if (!$process->isSuccessful()) {
            throw new \ErrorException('Export process failed: ' . $process->getErrorOutput());
        }
        // log the process output
        $output = $process->getOutput();
        $this->logger->info($output);

        $fetchedPos = strpos($output, "Fetched");
        $rowCountStr = substr($output, $fetchedPos, strpos($output, "rows in") - $fetchedPos);
        $linesWritten = (int) filter_var(
            $rowCountStr,
            FILTER_SANITIZE_NUMBER_INT
        );
        return $linesWritten;
    }

    public function testConnection(): bool
    {
        $cmd = [
            'java',
            '-jar',
            '/code/oracle/table-exporter.jar',
            'testConnection',
            $this->dataDir . "/" . self::TABLELESS_CONFIG_FILE
        ];

        $process = new Process($cmd);
        $process->setTimeout(null);
        $process->setIdleTimeout(null);
        $process->run();

        if (!$process->isSuccessful()) {
            throw new UserException('Failed connecting to DB: ' . $process->getErrorOutput());
        }
        return true;
    }

    public function getTables(array $tables = null): array
    {
        $this->prepareTablesConfig($tables);
        $cmd = [
            'java',
            '-jar',
            '/code/oracle/table-exporter.jar',
            'getTables',
            $this->dataDir . "/" . self::TABLES_CONFIG_FILE
        ];

        $process = new Process($cmd);
        $process->setTimeout(null);
        $process->setIdleTimeout(null);
        $process->run();

        if (!$process->isSuccessful()) {
            throw new UserException('Error fetching table listing: ' . $process->getErrorOutput());
        }

        $tableListing = json_decode(file_get_contents($this->dataDir . "/tables.json"), true);
        if (json_last_error() !== JSON_ERROR_NONE) {
            throw new ApplicationException(
                "Cannot parse JSON data of table listing - error: " . json_last_error()
            );
        }
        return $tableListing;
    }

    public function simpleQuery(array $table, array $columns = array()): string
    {
        if (count($columns) > 0) {
            return sprintf(
                "SELECT %s FROM %s.%s",
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
                "SELECT * FROM %s.%s",
                $this->quote($table['schema']),
                $this->quote($table['tableName'])
            );
        }
    }

    private function quote($obj)
    {
        return "\"{$obj}\"";
    }
}
