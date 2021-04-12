<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\ResultWriter;

use Iterator;
use Keboola\Component\UserException;
use Keboola\Csv\CsvWriter;
use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractor\Adapter\ValueObject\QueryMetadata;
use Keboola\DbExtractor\Adapter\ValueObject\QueryResult;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;

/**
 * Writes rows from the database to a CSV file + returns max value of inc. fetching column.
 */
class DefaultResultWriter implements ResultWriter
{
    public const MAX_VALUE_STATE_KEY = 'lastFetchedRow';

    protected array $state;

    protected ?string $fromEncoding = null;

    protected ?string $toEncoding = null;

    protected int $rowsCount;

    protected ?array $lastRow;

    public static function convertEncodingInArray(array $row, string $from, string $to): array
    {
        return array_map(
            fn($value) => iconv($from, $to, (string) $value),
            $row
        );
    }

    public function __construct(array $state)
    {
        $this->state = $state;
    }

    public function setIgnoreInvalidUtf8(): self
    {
        return $this->setConvertEncoding('UTF-8', 'UTF-8//IGNORE');
    }

    public function setConvertEncoding(string $fromEncoding, string $toEncoding): self
    {
        $this->fromEncoding = $fromEncoding;
        $this->toEncoding = $toEncoding;
        return $this;
    }

    public function writeToCsv(
        QueryResult $result,
        ExportConfig $exportConfig,
        string $csvFilePath
    ): ExportResult {
        $this->rowsCount = 0;
        $this->lastRow = null;

        // Create CSV writer
        $csvWriter = $this->createCsvWriter($csvFilePath);

        // Get iterator
        $iterator = $this->getIterator($result);

        // Write rows
        try {
            $this->writeRows($iterator, $result->getMetadata(), $exportConfig, $csvWriter);
        } finally {
            $result->closeCursor();
        }

        $incFetchingColMaxValue = $this->lastRow ?
            $this->getIncrementalFetchingValueFromLastRow($exportConfig) :
            $this->getIncrementalFetchingValueFromState($exportConfig);
        return new ExportResult($csvFilePath, $this->rowsCount, $incFetchingColMaxValue);
    }

    protected function writeRows(
        Iterator $iterator,
        QueryMetadata $queryMetadata,
        ExportConfig $exportConfig,
        CsvWriter $csvWriter
    ): void {
        // With custom query are no metadata in manifest, so header must be present
        $includeHeader = $exportConfig->hasQuery();

        // Write header
        if ($includeHeader) {
            $this->writeHeader($queryMetadata->getColumns()->getNames(), $csvWriter);
        }

        // Write the rest
        $this->rowsCount = 0;
        $this->lastRow = null;
        while ($iterator->valid()) {
            $resultRow = $iterator->current();
            $this->writeRow($resultRow, $csvWriter);
            $iterator->next();

            $this->lastRow = $resultRow;
            $this->rowsCount++;
        }
    }

    protected function writeHeader(array $header, CsvWriter $csvWriter): void
    {
        $this->writeRow($header, $csvWriter);
    }

    protected function writeRow(array $row, CsvWriter $csvWriter): void
    {
        if ($this->fromEncoding && $this->toEncoding) {
            $row = self::convertEncodingInArray($row, $this->fromEncoding, $this->toEncoding);
        }

        $csvWriter->writeRow($row);
    }

    protected function getIterator(QueryResult $result): Iterator
    {
        return $result->getIterator();
    }

    /**
     * @return mixed
     */
    protected function getIncrementalFetchingValueFromLastRow(ExportConfig $exportConfig)
    {
        // Get/check maximum (last) value of incremental fetching column
        $incFetchingColMaxValue = null;
        if ($exportConfig->isIncrementalFetching()) {
            $incrementalColumn = $exportConfig->getIncrementalFetchingConfig()->getColumn();
            if (!$this->lastRow || !array_key_exists($incrementalColumn, $this->lastRow)) {
                throw new UserException(
                    sprintf(
                        'The specified incremental fetching column %s not found in the table',
                        $incrementalColumn
                    )
                );
            }
            $incFetchingColMaxValue = (string) $this->lastRow[$incrementalColumn];
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

    protected function getIncrementalFetchingValueFromState(ExportConfig $exportConfig): ?string
    {
        return $exportConfig->isIncrementalFetching() && isset($this->state[self::MAX_VALUE_STATE_KEY]) ?
            (string) $this->state[self::MAX_VALUE_STATE_KEY] : null;
    }
}
