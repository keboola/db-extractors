<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\Table;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\TableCollection;
use Keboola\DbExtractorConfig\Configuration\ValueObject\InputTable;

class MySQLMetadataProvider implements MetadataProvider
{
    private ?string $database;

    public function __construct(?string $database)
    {
        $this->database = $database; // database is optional
    }

    public function getTable(InputTable $table): Table
    {
        // TODO: Implement getTable() method.
    }

    public function listTables(array $whitelist = [], bool $loadColumns = true): TableCollection
    {
        // TODO: Implement listTables() method.
    }
}
