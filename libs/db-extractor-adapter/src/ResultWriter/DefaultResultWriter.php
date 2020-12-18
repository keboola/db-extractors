<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\ResultWriter;

use Iterator;
use Keboola\Component\UserException;
use Keboola\Csv\CsvWriter;
use Keboola\DbExtractor\Adapter\Exception\NoRowsException;
use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractor\Adapter\ValueObject\QueryResult;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;

/**
 * Writes rows from the database to a CSV file + returns max value of inc. fetching column.
 */
class DefaultResultWriter implements ResultWriter
{
    public const MAX_VALUE_STATE_KEY = 'lastFetchedRow';

    protected array $state;

    protected int $rowsCount;

    protected ?array $lastRow;

    public function __construct(array $state)
    {
        $this->state = $state;
    }

    public function writeToCsv(QueryResult $result, ExportConfig $exportConfig, string $csvFilePath): ExportResult
    {
        $this->rowsCount = 0;
        $this->lastRow = null;

        // Create CSV writer
        $csvWriter = $this->createCsvWriter($csvFilePath);

        // Get iterator
        $iterator = $this->getIterator($result);

        // Write rows
        try {
            $this->writeRows($exportConfig, $iterator, $csvWriter);
        } catch (NoRowsException $e) {
            @unlink($csvFilePath); // no rows, no file
            return new ExportResult($csvFilePath, 0, $this->getMaxValueFromState($exportConfig));
        } finally {
            $result->closeCursor();
        }

        $incFetchingColMaxValue = $this->getIncrementalFetchingMaxValue($exportConfig, $this->lastRow);
        return new ExportResult($csvFilePath, $this->rowsCount, $incFetchingColMaxValue);
    }

    protected function writeRows(ExportConfig $exportConfig, Iterator $iterator, CsvWriter $csvWriter): void
    {
        // No rows found ?
        if (!$iterator->valid()) {
            throw new NoRowsException();
        }

        // With custom query are no metadata in manifest, so header must be present
        $includeHeader = $exportConfig->hasQuery();

        // Rows found, iterate!
        $resultRow = $iterator->current();
        $iterator->next();

        // Write header and first line
        if ($includeHeader) {
            $this->writeHeader(array_keys($resultRow), $csvWriter);
        }
        $this->writeRow($resultRow, $csvWriter);

        // Write the rest
        $this->rowsCount = 1;
        $this->lastRow = $resultRow;
        while ($iterator->valid()) {
            $resultRow = $iterator->current();
            $csvWriter->writeRow($resultRow);
            $iterator->next();

            $this->lastRow = $resultRow;
            $this->rowsCount++;
        }
    }

    protected function writeHeader(array $header, CsvWriter $csvWriter): void
    {
        $csvWriter->writeRow($header);
    }

    protected function writeRow(array $row, CsvWriter $csvWriter): void
    {
        $csvWriter->writeRow($row);
    }

    protected function getIterator(QueryResult $result): Iterator
    {
        return $result->getIterator();
    }

    /**
     * @return mixed
     */
    protected function getIncrementalFetchingMaxValue(ExportConfig $exportConfig, ?array $lastRow)
    {
        // Get/check maximum (last) value of incremental fetching column
        $incFetchingColMaxValue = null;
        if ($exportConfig->isIncrementalFetching()) {
            $incrementalColumn = $exportConfig->getIncrementalFetchingConfig()->getColumn();
            if (!$lastRow || !array_key_exists($incrementalColumn, $lastRow)) {
                throw new UserException(
                    sprintf(
                        'The specified incremental fetching column %s not found in the table',
                        $incrementalColumn
                    )
                );
            }
            $incFetchingColMaxValue = (string) $lastRow[$incrementalColumn];
        }

        return $incFetchingColMaxValue;
    }

    protected function createCsvWriter(string $csvFilePath): CsvWriter
    {
        $dir = dirname($csvFilePath);
        if (!is_dir($dir)) {
            mkdir($dir, 0777, true);
        }

        return new CsvWriter($csvFilePath);
    }

    protected function getMaxValueFromState(ExportConfig $exportConfig): ?string
    {
        return $exportConfig->isIncrementalFetching() && isset($this->state[self::MAX_VALUE_STATE_KEY]) ?
            (string) $this->state[self::MAX_VALUE_STATE_KEY] : null;
    }
}
