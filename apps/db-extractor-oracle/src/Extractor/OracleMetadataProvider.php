<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Datatype\Definition\Exception\InvalidTypeException;
use Keboola\Datatype\Definition\Oracle as OracleDatatype;
use Keboola\DbExtractor\Adapter\Metadata\MetadataProvider;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\MetadataBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\TableBuilder;
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
                ->setSchema($table['owner'])
                ->setCatalog($table['tablespaceName'] ?? null)
                ->setTablespaceName($table['tablespaceName'] ?? null)
                ->setOwner($table['owner'] ?? null)
                ->setDatatypeBackend('oracle');

            if ($loadColumns) {
                foreach ($table['columns'] as $column) {
                    $this->processColumn($tableBuilder, $column);
                }
            } else {
                $tableBuilder->setColumnsNotExpected();
            }
        }
        return $builder->build();
    }

    /**
     * @throws \Keboola\DbExtractor\TableResultFormat\Exception\InvalidStateException
     * @throws \Keboola\DbExtractor\TableResultFormat\Exception\InvalidArgumentException
     * @throws \Keboola\Datatype\Definition\Exception\InvalidOptionException
     * @throws \Keboola\Datatype\Definition\Exception\InvalidLengthException
     */
    private function processColumn(TableBuilder $tableBuilder, array $data): void
    {
        $columnType = $data['type'];
        $length = $data['length'];

        if (preg_match('/(.*)\((\d+|\d+,\d+)\)/', $columnType, $parsedType) === 1) {
            $columnType = $parsedType[1] ?? null;
            $length = $parsedType[2] ?? null;
        }

        try {
            $type = (new OracleDatatype($columnType))->getBasetype();
        } catch (InvalidTypeException $e) {
            $type = $columnType;
        }

        $tableBuilder
            ->addColumn()
            ->setName($data['name'])
            ->setType($type)
            ->setNullable($data['nullable'])
            ->setLength($length)
            ->setOrdinalPosition($data['ordinalPosition'])
            ->setPrimaryKey($data['primaryKey'])
            ->setUniqueKey($data['uniqueKey']);
    }
}
