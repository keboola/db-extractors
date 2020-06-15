<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Throwable;
use Psr\Log\LoggerInterface;
use Keboola\DbExtractor\TableResultFormat\Exception\ColumnNotFoundException;
use function Keboola\Utils\formatDateTime;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\GenericStorage;

class Oracle extends BaseExtractor
{
    public const INCREMENT_TYPE_NUMERIC = 'numeric';
    public const INCREMENT_TYPE_TIMESTAMP = 'timestamp';
    public const INCREMENT_TYPE_DATE = 'date';
    public const NUMERIC_BASE_TYPES = ['INTEGER', 'NUMERIC', 'FLOAT'];

    private OracleJavaExportWrapper $exportWrapper;

    private ?string $incrementalFetchingType = null;

    public function createConnection(array $params): void
    {
        // OracleJavaExportWrapper must be created after parent constructor,
        // ... because dbParameters are modified by SSH tunnel.
        $this->exportWrapper = new OracleJavaExportWrapper($this->logger, $this->dataDir, $this->getDbParameters());
    }

    public function getMetadataProvider(): MetadataProvider
    {
        return new OracleMetadataProvider($this->exportWrapper);
    }

    public function export(ExportConfig $exportConfig): array
    {
        if ($exportConfig->isIncrementalFetching()) {
            $this->validateIncrementalFetching($exportConfig);
        }

        $this->logger->info('Exporting to ' . $exportConfig->getOutputTable());

        // Max value
        $maxValue = $exportConfig->hasTable() && $exportConfig->isIncrementalFetching() ?
            $this->getMaxOfIncrementalFetchingColumn($exportConfig) : null;

        // Query
        $query = $exportConfig->hasQuery() ?
            rtrim($exportConfig->getQuery(), ' ;') :
            $this->simpleQuery($exportConfig);

        // Export
        try {
            $linesWritten = $this->exportWrapper->export(
                $query,
                $exportConfig->getMaxRetries(),
                $this->getOutputFilename($exportConfig->getOutputTable()),
                $exportConfig->hasQuery()
            );
        } catch (Throwable $e) {
            $logPrefix = $exportConfig->hasConfigName() ?
                $exportConfig->getConfigName() : $exportConfig->getOutputTable();
            throw $this->handleDbError($e, $exportConfig->getMaxRetries(), $logPrefix);
        }

        $rowCount = $linesWritten - 1;
        if ($rowCount > 0) {
            $this->createManifest($exportConfig);
        } else {
            @unlink($this->getOutputFilename($exportConfig->getOutputTable())); // no rows, no file
            $this->logger->warning(sprintf(
                'Query returned empty result. Nothing was imported to [%s]',
                $exportConfig->getOutputTable()
            ));
        }

        $output = [
            'outputTable' => $exportConfig->getOutputTable(),
            'rows' => $rowCount,
        ];

        // output state
        if ($maxValue) {
            $output['state']['lastFetchedRow'] = $maxValue;
        }

        return $output;
    }

    public function validateIncrementalFetching(ExportConfig $exportConfig): void
    {
        try {
            $column = $this
                ->getMetadataProvider()
                ->getTable($exportConfig->getTable())
                ->getColumns()
                ->getByName($exportConfig->getIncrementalFetchingColumn());
        } catch (ColumnNotFoundException $e) {
            throw new UserException(sprintf(
                'Column "%s" specified for incremental fetching was not found in the table.',
                $exportConfig->getIncrementalFetchingColumn()
            ));
        }

        try {
            $datatype = new GenericStorage($column->getType());
            if (in_array($datatype->getBasetype(), self::NUMERIC_BASE_TYPES)) {
                $this->incrementalFetchingType = self::INCREMENT_TYPE_NUMERIC;
            } else if ($datatype->getBasetype() === 'TIMESTAMP') {
                $this->incrementalFetchingType = self::INCREMENT_TYPE_TIMESTAMP;
            } else if ($datatype->getBasetype() === 'DATE') {
                $this->incrementalFetchingType = self::INCREMENT_TYPE_DATE;
            } else {
                throw new UserException('invalid incremental fetching column type');
            }
        } catch (InvalidLengthException | UserException $exception) {
            throw new UserException(
                sprintf(
                    'Column "%s" specified for incremental fetching is not a numeric or timestamp type column.',
                    $column->getName()
                )
            );
        }
    }

    public function getMaxOfIncrementalFetchingColumn(ExportConfig $exportConfig): ?string
    {
        $outputFile = $this->getOutputFilename('last_row');
        $this->exportWrapper->export(
            $this->getLastRowQuery($exportConfig),
            $exportConfig->getMaxRetries(),
            $outputFile,
            false
        );

        $value = json_decode((string) file_get_contents($outputFile));
        unlink($outputFile);
        return $value;
    }


    public function testConnection(): void
    {
        $this->exportWrapper->testConnection();
    }


    public function simpleQuery(ExportConfig $exportConfig): string
    {
        $sql = [];
        $where = [];

        if ($exportConfig->hasColumns()) {
            $sql[] = sprintf('SELECT %s', implode(', ', array_map(
                fn(string $c) => $this->quote($c),
                $exportConfig->getColumns()
            )));
        } else {
            $sql[] = 'SELECT *';
        }

        $sql[] = sprintf(
            'FROM %s.%s',
            $this->quote($exportConfig->getTable()->getSchema()),
            $this->quote($exportConfig->getTable()->getName())
        );

        if ($exportConfig->isIncrementalFetching() && isset($this->state['lastFetchedRow'])) {
            if ($this->incrementalFetchingType === self::INCREMENT_TYPE_NUMERIC) {
                $lastFetchedRow = $this->state['lastFetchedRow'];
            } else if ($this->incrementalFetchingType === self::INCREMENT_TYPE_DATE) {
                $lastFetchedRow = sprintf(
                    "DATE '%s'",
                    formatDateTime($this->state['lastFetchedRow'], 'Y-m-d')
                );
            } else {
                $lastFetchedRow = $this->quote((string) $this->state['lastFetchedRow']);
            }

            // intentionally ">=" last row should be included, it is handled by storage deduplication process
            $where[] = sprintf(
                '%s >= %s',
                $this->quote($exportConfig->getIncrementalFetchingColumn()),
                $lastFetchedRow
            );
        }

        if ($exportConfig->hasIncrementalFetchingLimit()) {
            $where[] = sprintf('ROWNUM <= %d', $exportConfig->getIncrementalFetchingLimit());
        }

        if ($where) {
            $sql[] = sprintf('WHERE %s', implode(' AND ', $where));
        }

        return implode(' ', $sql);
    }

    private function getLastRowQuery(ExportConfig $exportConfig): string
    {
        return sprintf(
            'SELECT %s FROM (SELECT * FROM (%s) ORDER BY %s DESC) WHERE ROWNUM = 1',
            $this->quote($exportConfig->getIncrementalFetchingColumn()),
            $this->simpleQuery($exportConfig),
            $this->quote($exportConfig->getIncrementalFetchingColumn()),
        );
    }

    private function quote(string $obj): string
    {
        return "\"{$obj}\"";
    }
}
