<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\MetadataBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\Table;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\TableCollection;
use Keboola\DbExtractorConfig\Configuration\ValueObject\InputTable;

class OracleMetadataProvider implements MetadataProvider
{
    private OracleJavaExportWrapper $exportWrapper;

    public function __construct(OracleJavaExportWrapper $exportWrapper)
    {
        $this->exportWrapper = $exportWrapper;
    }

    public function getTable(InputTable $table): Table
    {
        return $this
            ->listTables([$table])
            ->getByNameAndSchema($table->getName(), $table->getSchema());
    }

    public function listTables(array $whitelist = [], bool $loadColumns = true): TableCollection
    {
        $tables = $this->exportWrapper->getTables($whitelist, $loadColumns);
        $builder = MetadataBuilder::create();

        foreach ($tables as $table) {
            $tableBuilder = $builder
                ->addTable()
                ->setName($table['name'])
                ->setSchema($table['schema'])
                ->setCatalog($table['tablespaceName'] ?? null);

            if ($loadColumns) {
                foreach ($table['columns'] as $column) {
                    $tableBuilder
                        ->addColumn()
                        ->setName($column['name'])
                        ->setType($column['type'])
                        ->setNullable($column['nullable'])
                        ->setLength($column['length'])
                        ->setOrdinalPosition($column['ordinalPosition'])
                        ->setPrimaryKey($column['primaryKey'])
                        ->setUniqueKey($column['uniqueKey']);
                }
            } else {
                $tableBuilder->setColumnsNotExpected();
            }
        }
        return $builder->build();
    }
}
