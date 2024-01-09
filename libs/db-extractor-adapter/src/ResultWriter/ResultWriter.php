<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\ResultWriter;

use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractor\Adapter\ValueObject\QueryResult;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;

/**
 * Writes rows from the database to a CSV file + returns max value of inc. fetching column.
 */
interface ResultWriter
{
    public function writeToCsv(QueryResult $result, ExportConfig $exportConfig, string $csvFilePath): ExportResult;
}
