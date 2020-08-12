<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter;

use Keboola\Component\UserException;
use Keboola\Csv\CsvWriter;
use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractor\Adapter\ValueObject\QueryResult;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;

/**
 * This class sequentially writes rows from the database to a CSV file + returns max value of inc. fetching column.
 */
class QueryResultCsvWriter
{
    protected string $dataDir;

    protected array $state;

    public function __construct(string $dataDir, array $state)
    {
        $this->dataDir = $dataDir;
        $this->state = $state;
    }

    public function writeToCsv(QueryResult $result, ExportConfig $exportConfig, string $csvFileName): ExportResult
    {
        // Create CSV writer
        $csvFilePath = $this->getCsvFilePath($csvFileName);
        $csv = $this->createCsvWriter($csvFilePath);

        // With custom query are no metadata in manifest, so header must be present
        $includeHeader = $exportConfig->hasQuery();

        // No rows found.  If incremental fetching is turned on, we need to preserve the last state
        $iterator = $result->getIterator();
        if (!$iterator->valid()) {
            $result->closeCursor();
            unlink($csvFilePath); // no rows, no file

            $incFetchingColMaxValue = $exportConfig->isIncrementalFetching() && isset($this->state['lastFetchedRow']) ?
                (string) $this->state['lastFetchedRow'] : null;
            return new ExportResult($csvFilePath, 0, $incFetchingColMaxValue);
        }

        // Rows found, iterate!
        $resultRow = $iterator->current();
        $iterator->next();

        // Write header and first line
        if ($includeHeader) {
            $csv->writeRow(array_keys($resultRow));
        }
        $csv->writeRow($resultRow);

        // Write the rest
        $numRows = 1;
        $lastRow = $resultRow;
        while ($iterator->valid()) {
            $resultRow = $iterator->current();
            $csv->writeRow($resultRow);
            $iterator->next();

            $lastRow = $resultRow;
            $numRows++;
        }
        $result->closeCursor();

        // Get/check maximum (last) value of incremental fetching column
        $incFetchingColMaxValue = null;
        if ($exportConfig->isIncrementalFetching()) {
            $incrementalColumn = $exportConfig->getIncrementalFetchingConfig()->getColumn();
            if (!array_key_exists($incrementalColumn, $lastRow)) {
                throw new UserException(
                    sprintf(
                        'The specified incremental fetching column %s not found in the table',
                        $incrementalColumn
                    )
                );
            }
            $incFetchingColMaxValue = (string) $lastRow[$incrementalColumn];
        }

        return new ExportResult($csvFilePath, $numRows, $incFetchingColMaxValue);
    }

    protected function getCsvFilePath(string $csvFileName): string
    {
        $outTablesDir = $this->dataDir . '/out/tables';
        if (!is_dir($outTablesDir)) {
            mkdir($outTablesDir, 0777, true);
        }

        return $outTablesDir . '/' . $csvFileName;
    }

    protected function createCsvWriter(string $csvFilePath): CsvWriter
    {
        return new CsvWriter($csvFilePath);
    }
}
