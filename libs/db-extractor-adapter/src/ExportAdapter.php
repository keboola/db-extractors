<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter;

use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;

/**
 * This class exports and saves data to a CSV file according to the specified configuration.
 * It uses various tools for this purpose, for example: PDO, ODBC, cli BCP tool, ...
 */
interface ExportAdapter
{
    public function getName(): string;

    public function export(ExportConfig $exportConfig, string $csvFileName): ExportResult;
}
