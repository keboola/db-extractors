<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TableResultFormat\Metadata\GetTables;

use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\TableCollection;

interface GetTablesSerializer
{
    public function serialize(TableCollection $tables): array;
}
