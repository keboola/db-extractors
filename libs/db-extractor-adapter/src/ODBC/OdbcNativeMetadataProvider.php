<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\ODBC;

use Keboola\Datatype\Definition\GenericStorage;
use Keboola\DbExtractor\Adapter\Metadata\MetadataProvider;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\ColumnBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\MetadataBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\TableBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\Table;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\TableCollection;
use Keboola\DbExtractorConfig\Configuration\ValueObject\InputTable;

/**
 * Returns table and column metadata using the PHP functions: "odbc_tables" and "odbc_columns".
 */
class OdbcNativeMetadataProvider implements MetadataProvider
{
    private OdbcConnection $connection;

    private string $onlyFromCatalog;

    private string $onlyFromSchema;

    private array $ignoredCatalogs;

    private array $ignoredSchemas;

    private array $typesWithoutLength;

    public function __construct(
        OdbcConnection $connection,
        ?string $onlyFromCatalog = null,
        ?string $onlyFromSchema = null,
        array $ignoredCatalogs = [],
        array $ignoredSchemas = [],
        array $typesWithoutLength = []
    ) {
        $this->connection = $connection;
        $this->onlyFromCatalog = $onlyFromCatalog ?? '';
        $this->onlyFromSchema = $onlyFromSchema ?? '';
        $this->ignoredCatalogs = $ignoredCatalogs;
        $this->ignoredSchemas = $ignoredSchemas;
        $this->typesWithoutLength = $typesWithoutLength ?: $this->getTypesWithoutLength();
    }

    public function getTable(InputTable $table): Table
    {
        return $this
            ->listTables([$table])
            ->getByNameAndSchema($table->getName(), $table->getSchema());
    }

    /**
     * @param array|InputTable[] $whitelist
     */
    public function listTables(array $whitelist = [], bool $loadColumns = true): TableCollection
    {
        /** @var TableBuilder[] $tableBuilders */
        $tableBuilders = [];

        /** @var ColumnBuilder[] $columnBuilders */
        $columnBuilders = [];

        // Process tables
        $tableRequiredProperties = ['type'];
        $columnRequiredProperties= ['ordinalPosition', 'nullable'];
        $builder = MetadataBuilder::create($tableRequiredProperties, $columnRequiredProperties);
        foreach ($this->queryTables($whitelist) as $data) {
            $tableId = $this->getTableId($data);
            $tableBuilder = $builder->addTable();
            $tableBuilders[$tableId] = $tableBuilder;

            if ($loadColumns === false) {
                $tableBuilder->setColumnsNotExpected();
            }

            $this->processTableData($tableBuilder, $data);
        }

        // Process columns
        if ($loadColumns) {
            foreach ($this->queryColumns($whitelist) as $data) {
                $tableId = $this->getTableId($data);
                $columnId = $tableId . '.' . $data['COLUMN_NAME'];

                // If the column has multiple constraints
                // ... then is present multiple times in results
                if (isset($columnBuilders[$columnId])) {
                    $columnBuilder = $columnBuilders[$columnId];
                } else {
                    $columnBuilder = $tableBuilders[$tableId]->addColumn();
                    $columnBuilders[$columnId] = $columnBuilder;
                }

                $this->processColumnData($columnBuilder, $data);
            }
        }

        return $builder->build();
    }

    protected function processColumnData(ColumnBuilder $builder, array $data): void
    {
        $type = $data['TYPE_NAME'];
        if (!in_array(strtolower($type), $this->typesWithoutLength)) {
            $length = $data['COLUMN_SIZE'] ?? null;
            if ($length && isset($data['NUM_PREC_RADIX'])) {
                $length .= ',' . $data['NUM_PREC_RADIX'];
            }
        } else {
            $length = null;
        }

        $builder->setOrdinalPosition((int) $data['ORDINAL_POSITION']);
        $builder->setName($data['COLUMN_NAME']);
        $builder->setType($type);
        $builder->setLength($length);
        $builder->setNullable((int) $data['NULLABLE'] === 1);
        $builder->setDefault($data['COLUMN_DEF']);
    }


    protected function processTableData(TableBuilder $builder, array $data): void
    {
        $builder->setName($data['TABLE_NAME']);
        $builder->setCatalog($data['TABLE_CAT'] ?? null);
        $builder->setSchema($data['TABLE_SCHEM'] ?? null);
        $builder->setType(strpos($data['TABLE_TYPE'], 'VIEW') !== false ? 'view' : 'table');
    }

    protected function getTableId(array $data): string
    {
        return ($data['TABLE_CAT'] ?? '') . ($data['TABLE_SCHEM'] ?? '') . '.' . $data['TABLE_NAME'];
    }

    /**
     * @param array|InputTable[] $whitelist
     * @return mixed[][]
     */
    protected function queryColumns(array $whitelist): array
    {
        $whitelist = empty($whitelist) ? [null] : $whitelist;
        $columns = [];

        foreach ($whitelist as $whitelistedTable) {
            $result = odbc_columns(
                $this->connection->getConnection(),
                $this->onlyFromCatalog,
                $this->onlyFromSchema,
                // % means ALL, see odbc_columns docs
                $whitelistedTable ? $whitelistedTable->getName() : '%',
            );
            while ($column = odbc_fetch_array($result)) {
                if ($this->isTableIgnored($column)) {
                    continue;
                }
                $columns[] = $column;
            }
            odbc_free_result($result);
        }

        // Sort
        usort($columns, function (array $a, array $b) {
            $aId = $this->getTableId($a) . '_' . $a['ORDINAL_POSITION'];
            $bId = $this->getTableId($b) . '_' . $b['ORDINAL_POSITION'];
            return strnatcmp($aId, $bId);
        });

        return $columns;
    }

    /**
     * @param array|InputTable[] $whitelist
     * @return mixed[][]
     */
    protected function queryTables(array $whitelist): array
    {
        $whitelist = empty($whitelist) ? [null] : $whitelist;
        $tables = [];

        foreach ($whitelist as $whitelistedTable) {
            $result = odbc_tables(
                $this->connection->getConnection(),
                $this->onlyFromCatalog,
                $this->onlyFromSchema,
                // % means ALL, see odbc_tables docs
                $whitelistedTable ? $whitelistedTable->getName() : '%'
            );
            while ($table = odbc_fetch_array($result)) {
                if ($this->isTableIgnored($table)) {
                    continue;
                }
                $tables[] = $table;
            }
            odbc_free_result($result);
        }

        // Sort
        usort($tables, function (array $a, array $b) {
            $aId = $this->getTableId($a);
            $bId = $this->getTableId($b);
            return strnatcmp($aId, $bId);
        });

        return $tables;
    }

    protected function isTableIgnored(array $data): bool
    {
        if (isset($data['TABLE_CAT']) && in_array($data['TABLE_CAT'], $this->ignoredCatalogs, true)) {
            return true;
        }

        if (isset($data['TABLE_SCHEM']) && in_array($data['TABLE_SCHEM'], $this->ignoredSchemas, true)) {
            return true;
        }

        return false;
    }

    protected function getTypesWithoutLength(): array
    {
        return array_merge(
            GenericStorage::DATE_TYPES,
            GenericStorage::TIMESTAMP_TYPES,
            GenericStorage::INTEGER_TYPES,
            GenericStorage::FLOATING_POINT_TYPES,
            GenericStorage::BOOLEAN_TYPES
        );
    }
}
