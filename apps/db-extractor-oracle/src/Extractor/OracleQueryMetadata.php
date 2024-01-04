<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Adapter\ValueObject\QueryMetadata;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\ColumnCollection;

class OracleQueryMetadata implements QueryMetadata
{
    public function getColumns(): ColumnCollection
    {
        return new ColumnCollection([]);
    }
}
