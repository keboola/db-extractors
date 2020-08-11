<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\Query;

use Keboola\DbExtractor\Adapter\Connection\DbConnection;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;

interface SimpleQueryFactory
{
    public function create(ExportConfig $exportConfig, DbConnection $connection): string;
}
