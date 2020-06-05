<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use function Keboola\Utils\formatDateTime;
use Throwable;
use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\DbExtractor\DbRetryProxy;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\OracleJavaExportException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\TableResultFormat\Table;
use Keboola\DbExtractor\TableResultFormat\TableColumn;
use Keboola\DbExtractorLogger\Logger;

class Oracle extends Extractor
{
    public const INCREMENT_TYPE_NUMERIC = 'numeric';
    public const INCREMENT_TYPE_TIMESTAMP = 'timestamp';
    public const INCREMENT_TYPE_DATE = 'date';
    public const NUMERIC_BASE_TYPES = ['INTEGER', 'NUMERIC', 'FLOAT'];

    private array $parameters;

    private OracleJavaExportWrapper $exportWrapper;

    public function __construct(array $parameters, array $state, Logger $logger)
    {
        $this->parameters = $parameters;
        $this->exportWrapper = new OracleJavaExportWrapper($logger, $parameters['data_dir'], $parameters['db']);
        parent::__construct($parameters, $state, $logger);
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

        $query = isset($table['query']) ?
            rtrim($table['query'], ' ;') :
            $this->simpleQuery(
                $table['table'],
                isset($table['columns']) ? $table['columns'] : []
            );
        $maxTries = isset($table['retries']) ? (int) $table['retries'] : DbRetryProxy::DEFAULT_MAX_TRIES;
        $outputFile = $this->getOutputFilename($table['outputTable']);

        try {
            $linesWritten = $this->exportWrapper->export($query, $maxTries, $outputFile, $isAdvancedQuery);
        } catch (OracleJavaExportException $e) {
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
        $outputFile = $this->getOutputFilename('last_row');
        $maxTries = isset($table['retries']) ? (int) $table['retries'] : DbRetryProxy::DEFAULT_MAX_TRIES;

        $simplyQuery = $this->simpleQuery(
            $table['table'],
            [$this->incrementalFetching['column']]
        );

        try {
            $this->exportWrapper->export(
                $this->getLastRowQuery($simplyQuery),
                $maxTries,
                $outputFile,
                false,
            );

            return json_decode((string) file_get_contents($outputFile));
        } finally {
            @unlink($outputFile);
        }
    }

    public function testConnection(): bool
    {
        $this->exportWrapper->testConnection();
        return true;
    }

    public function getTables(?array $tables = null): array
    {
        $loadColumns = $this->parameters['tableListFilter']['listColumns'] ?? true;
        $whiteList = $this->parameters['tableListFilter']['tablesToList'] ?? [];
        $tables = $whiteList && !$tables ? $whiteList : $tables;
        $tableListing =  $this->exportWrapper->getTables($tables ?? [], $loadColumns);

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
        $sql = [];
        $where = [];

        if (count($columns) > 0) {
            $sql[] = sprintf('SELECT %s', implode(', ', array_map(
                fn(string $c) => $this->quote($c),
                $columns
            )));
        } else {
            $sql[] = 'SELECT *';
        }

        $sql[] = sprintf(
            'FROM %s.%s',
            $this->quote($table['schema']),
            $this->quote($table['tableName'])
        );

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

                // intentionally ">=" last row should be included, it is handled by storage deduplication process
                $where[] = sprintf(
                    '%s >= %s',
                    $this->quote($this->incrementalFetching['column']),
                    $lastFetchedRow
                );
            }

            if (isset($this->incrementalFetching['limit'])) {
                $where[] = sprintf(
                    'ROWNUM <= %d',
                    $this->incrementalFetching['limit']
                );
            }
        }

        if ($where) {
            $sql[] = sprintf('WHERE %s', implode(' AND ', $where));
        }

        return implode(' ', $sql);
    }

    private function getLastRowQuery(string $query): string
    {
        return sprintf(
            'SELECT %s FROM (SELECT * FROM (%s) ORDER BY %s DESC) WHERE ROWNUM = 1',
            $this->quote($this->incrementalFetching['column']),
            $query,
            $this->quote($this->incrementalFetching['column']),
        );
    }

    private function quote(string $obj): string
    {
        return "\"{$obj}\"";
    }
}
