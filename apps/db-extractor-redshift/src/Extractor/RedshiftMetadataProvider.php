<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Adapter\Metadata\MetadataProvider;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\ColumnBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\MetadataBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\TableBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\Table;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\TableCollection;
use Keboola\DbExtractorConfig\Configuration\ValueObject\InputTable;
use PDOStatement;

class RedshiftMetadataProvider implements MetadataProvider
{
    private RedshiftPdoConnection $db;

    public function __construct(RedshiftPdoConnection $db)
    {
        $this->db = $db;
    }

    public function getTable(InputTable $table): Table
    {
        return $this
            ->listTables([$table])
            ->getByNameAndSchema($table->getName(), $table->getSchema());
    }

    /**
     * @param array|InputTable[] $whitelist
     * @param bool $loadColumns if false, columns metadata are NOT loaded, useful useful if there are a lot of tables
     */
    public function listTables(array $whitelist = [], bool $loadColumns = true): TableCollection
    {
        $tableBuilders = [];

        // Process tables
        $tableRequiredProperties = ['schema', 'type'];
        $columnRequiredProperties= ['ordinalPosition', 'nullable'];

        $builder = MetadataBuilder::create($tableRequiredProperties, $columnRequiredProperties);
        $nameTables = [];
        foreach ($this->queryTables($whitelist) as $item) {
            $tableId = $item['table_schema'] . '.' . $item['table_name'];
            $tableBuilder = $builder->addTable();
            $tableBuilders[$tableId] = $tableBuilder;

            if ($loadColumns === false) {
                $tableBuilder->setColumnsNotExpected();
            }

            $this->processTableData($tableBuilder, $item);
            $nameTables[] = $item['table_name'];
        }

        if ($loadColumns) {
            foreach ($this->queryColumns($nameTables) as $column) {
                $tableId = $column['table_schema'] . '.' . $column['table_name'];
                if (!isset($tableBuilders[$tableId])) {
                    continue;
                }
                $columnBuilder = $tableBuilders[$tableId]->addColumn();
                $this->processColumnData($columnBuilder, $column);
            }

            foreach ($this->queryLateBindViewsColumns() as $column) {
                $tableId = $column['view_schema'] . '.' . $column['view_name'];
                if (!isset($tableBuilders[$tableId])) {
                    continue;
                }
                $columnBuilder = $tableBuilders[$tableId]->addColumn();

                $columnBuilder
                    ->setName($column['col_name'])
                    ->setType($column['col_type'])
                    ->setNullable(false)
                    ->setOrdinalPosition($column['col_num']);
            }
        }

        return $builder->build();
    }

    private function processTableData(TableBuilder $builder, array $data): void
    {
        $builder
            ->setSchema($data['table_schema'])
            ->setName($data['table_name'])
            ->setCatalog($data['table_catalog'] ?? null)
            ->setType($data['table_type'] ?? 'view')
        ;
    }

    private function queryColumns(array $nameTables): iterable
    {
        $sqlTemplate = <<<SQL
SELECT cols.column_name, cols.table_name, cols.table_schema, 
        cols.column_default, cols.is_nullable, cols.data_type, cols.ordinal_position,
        cols.character_maximum_length, cols.numeric_precision, cols.numeric_scale,
        def.contype, def.conkey
FROM information_schema.columns as cols 
JOIN (
  SELECT
    a.attnum,
    n.nspname,
    c.relname,
    a.attname AS colname,
    t.typname AS type,
    a.atttypmod,
    FORMAT_TYPE(a.atttypid, a.atttypmod) AS complete_type,
    d.adsrc AS default_value,
    a.attnotnull AS notnull,
    a.attlen AS length,
    co.contype,
    ARRAY_TO_STRING(co.conkey, ',') AS conkey
  FROM pg_attribute AS a
    JOIN pg_class AS c ON a.attrelid = c.oid
    JOIN pg_namespace AS n ON c.relnamespace = n.oid
    JOIN pg_type AS t ON a.atttypid = t.oid
    LEFT OUTER JOIN pg_constraint AS co ON (co.conrelid = c.oid
        AND a.attnum = ANY(co.conkey) AND (co.contype = 'p' OR co.contype = 'u'))
    LEFT OUTER JOIN pg_attrdef AS d ON d.adrelid = c.oid AND d.adnum = a.attnum
  WHERE a.attnum > 0 AND c.relname IN (%s)
) as def 
ON cols.column_name = def.colname AND cols.table_name = def.relname
WHERE cols.table_name IN (%s) ORDER BY cols.table_schema, cols.table_name, cols.ordinal_position
SQL;

        $sql = sprintf(
            $sqlTemplate,
            implode(', ', array_map(function (string $tableName) {
                return $this->db->quote($tableName);
            }, $nameTables)),
            implode(', ', array_map(function (string $tableName) {
                return $this->db->quote($tableName);
            }, $nameTables))
        );

        return $this->queryAndFetchAll($sql);
    }

    private function queryLateBindViewsColumns(): iterable
    {
        $sql = <<<SQL
select * from pg_get_late_binding_view_cols()
    cols(view_schema name, view_name name, col_name name, col_type varchar, col_num int);
SQL;

        return $this->queryAndFetchAll($sql);
    }

    private function queryTables(?array $whiteList): iterable
    {
        $sql = [];
        $sql[] = 'SELECT * FROM information_schema.tables';
        $sql[] = 'WHERE table_schema != \'pg_catalog\'';
        $sql[] = 'AND table_schema != \'information_schema\'';
        $sql[] = 'AND table_schema != \'pg_internal\'';

        if ($whiteList) {
            $whiteListSql = array_map(function (InputTable $v) {
                return sprintf(
                    '(table_schema = %s AND table_name = %s)',
                    $this->db->quote($v->getSchema()),
                    $this->db->quote($v->getName())
                );
            }, $whiteList);

            $sql[] = sprintf(
                'AND %s',
                implode(' OR ', $whiteListSql)
            );
        }

        $sql[] = 'ORDER BY table_schema, table_name';

        return $this->queryAndFetchAll(implode(' ', $sql));
    }

    private function processColumnData(ColumnBuilder $columnBuilder, array $column): void
    {
        $length = ($column['character_maximum_length']) ? $column['character_maximum_length'] : null;
        if (is_null($length) && !is_null($column['numeric_precision'])) {
            if ($column['numeric_scale'] > 0) {
                $length = $column['numeric_precision'] . ',' . $column['numeric_scale'];
            } else {
                $length = $column['numeric_precision'];
            }
        }
        $default = null;
        if (!is_null($column['column_default'])) {
            $default = str_replace("'", '', explode('::', $column['column_default'])[0]);
        }

        $columnBuilder
            ->setName($column['column_name'])
            ->setType($column['data_type'])
            ->setDefault($default)
            ->setLength((string) $length)
            ->setNullable((trim($column['is_nullable']) === 'NO') ? false : true)
            ->setOrdinalPosition((int) $column['ordinal_position'])
            ->setPrimaryKey(($column['contype'] === 'p') ? true : false)
            ->setUniqueKey(($column['contype'] === 'u') ? true : false)
        ;
    }

    private function queryAndFetchAll(string $sql): iterable
    {
        /** @var PDOStatement $result */
        $result = $this->db->query($sql);
        while ($row = $result->fetch()) {
            yield $row;
        }
    }
}
