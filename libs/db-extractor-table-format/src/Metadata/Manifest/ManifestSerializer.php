<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TableResultFormat\Metadata\Manifest;

use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\Column;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\Table;

interface ManifestSerializer
{
    public function serializeTable(Table $table): array;

    public function serializeColumn(Column $column): array;
}
