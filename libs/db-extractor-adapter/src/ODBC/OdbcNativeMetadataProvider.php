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

    private ?string $onlyFromCatalog;

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
        $this->onlyFromCatalog = $onlyFromCatalog;
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

        /** @var int[] $ordinalPosition */
        $ordinalPosition = [];

        // Process tables
        $tableRequiredProperties = ['type'];
        $columnRequiredProperties = [];
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
            $primaryKeys = $this->queryPrimaryKeys($whitelist);
            foreach ($this->queryColumns($whitelist) as $data) {
                $tableId = $this->getTableId($data);
                $columnId = $this->getColumnId($data);

                // If the column has multiple constraints
                // ... then is present multiple times in results
                if (isset($columnBuilders[$columnId])) {
                    $columnBuilder = $columnBuilders[$columnId];
                } else {
                    $columnBuilder = $tableBuilders[$tableId]->addColumn();
                    $columnBuilders[$columnId] = $columnBuilder;
                }

                $ordinalPosition[$tableId] = ($ordinalPosition[$tableId] ?? 0) + 1;
                $primaryKey = array_key_exists($columnId, $primaryKeys);
                $this->processColumnData($columnBuilder, $data, $ordinalPosition[$tableId], $primaryKey);
            }
        }

        return $builder->build();
    }

    protected function processColumnData(
        ColumnBuilder $builder,
        array $data,
        int $ordinalPosition,
        bool $primaryKey
    ): void {
        $type = $data['TYPE_NAME'];
        if (!in_array(strtolower($type), $this->typesWithoutLength)) {
            $length = $data['COLUMN_SIZE'] ?? null;
            if ($length && isset($data['DECIMAL_DIGITS'])) {
                $length .= ',' . $data['DECIMAL_DIGITS'];
            }
        } else {
            $length = null;
        }

        if (isset($data['ORDINAL_POSITION'])) {
            $builder->setOrdinalPosition((int) $data['ORDINAL_POSITION']);
        } else {
            $builder->setOrdinalPosition($ordinalPosition);
        }

        if (isset($data['NULLABLE'])) {
            $builder->setNullable((int) $data['NULLABLE'] === 1);
        }

        if (array_key_exists('COLUMN_DEF', $data)) {
            $builder->setDefault($data['COLUMN_DEF']);
        }

        if ($primaryKey) {
            $builder->setPrimaryKey(true);
        }

        $builder->setName($data['COLUMN_NAME']);
        $builder->setType($type);
        $builder->setLength($length);
    }


    protected function processTableData(TableBuilder $builder, array $data): void
    {
        $builder->setName($data['TABLE_NAME']);
        $builder->setCatalog($data['TABLE_CAT'] ?? $data['TABLE_QUALIFIER'] ?? null);
        $builder->setSchema($data['TABLE_SCHEM'] ?? $data['TABLE_OWNER'] ?? null);
        $builder->setType(strpos($data['TABLE_TYPE'], 'VIEW') !== false ? 'view' : 'table');
    }


    protected function getColumnId(array $data): string
    {
        return $this->getTableId($data) . '.' . $data['COLUMN_NAME'];
    }

    protected function getTableId(array $data): string
    {
        return ($data['TABLE_CAT'] ?? '') . ($data['TABLE_SCHEM'] ?? '') . '.' . $data['TABLE_NAME'];
    }

    /**
     * @param array|InputTable[] $whitelist
     * @return array[string][]
     */
    protected function queryPrimaryKeys(array $whitelist): array
    {
        $whitelist = empty($whitelist) ? [null] : $whitelist;
        $pks = [];

        foreach ($whitelist as $whitelistedTable) {
            $result = odbc_primarykeys(
                $this->connection->getConnection(),
                $this->onlyFromCatalog,
                $this->onlyFromSchema,
                // % means ALL, see odbc_columns docs
                $whitelistedTable ? $whitelistedTable->getName() : '%',
            );
            while ($pk = odbc_fetch_array($result)) {
                if ($this->isTableIgnored($pk)) {
                    continue;
                }
                $pks[$this->getColumnId($pk)] = $pk;
            }
            odbc_free_result($result);
        }

        return $pks;
    }

    /**
     * @param array|InputTable[] $whitelist
     * @return mixed[][]
     */
    protected function queryColumns(array $whitelist): array
    {
        $whitelist = empty($whitelist) ? [null] : $whitelist;
        $columns = [];

        $i = 0;
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
                $column['_index'] = $i++;
                $columns[] = $column;
            }
            odbc_free_result($result);
        }

        // Sort
        usort($columns, function (array $a, array $b) {
            // If not ordinal_position is not set, then the order in which the values were read is taken.
            $aId = $this->getTableId($a) . '_' . ($a['ORDINAL_POSITION'] ?? $a['_index']);
            $bId = $this->getTableId($b) . '_' . ($b['ORDINAL_POSITION'] ?? $b['_index']);
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
