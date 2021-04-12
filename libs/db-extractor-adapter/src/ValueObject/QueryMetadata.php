<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\ValueObject;

use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\ColumnCollection;

/**
 * Export metadata: header, ...
 */
interface QueryMetadata
{
    public function getColumns(): ColumnCollection;
}
