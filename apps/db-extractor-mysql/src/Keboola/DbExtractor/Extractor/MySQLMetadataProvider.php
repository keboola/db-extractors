<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\TableResultFormat\Exception\InvalidStateException;
use PDO;
use PDOStatement;
use Keboola\DbExtractor\Exception\InvalidArgumentException;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\ColumnBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\MetadataBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\TableBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\Table;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\TableCollection;
use Keboola\DbExtractorConfig\Configuration\ValueObject\InputTable;

class MySQLMetadataProvider implements MetadataProvider
{
    private PDO $db;

    private ?string $database;

    public function __construct(PDO $db, ?string $database)
    {
        $this->db = $db;
        $this->database = $database; // database is optional
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
        /** @var TableBuilder[] $tableBuilders */
        $tableBuilders = [];

        /** @var ColumnBuilder[] $columnBuilders */
        $columnBuilders = [];

        /** @var int[] $autoIncrements */
        $autoIncrements = [];

        // Process tables
        $tableRequiredProperties = ['schema', 'type', 'rowCount'];
        $columnRequiredProperties= ['ordinalPosition', 'nullable'];
        $builder = MetadataBuilder::create($tableRequiredProperties, $columnRequiredProperties);
        foreach ($this->queryTables($whitelist) as $data) {
            $tableId = $data['TABLE_SCHEMA'] . '.' . $data['TABLE_NAME'];
            $tableBuilder = $builder->addTable();
            $tableBuilders[$tableId] = $tableBuilder;

            if ($loadColumns === false) {
                $tableBuilder->setColumnsNotExpected();
            }

            $this->processTableData($tableBuilder, $data);
            $autoIncrements[$tableId] = $data['AUTO_INCREMENT'] ?: null;
        }

        // Process columns
        if ($loadColumns) {
            foreach ($this->queryColumns($whitelist) as $data) {
                $tableId = $data['TABLE_SCHEMA'] . '.' . $data['TABLE_NAME'];
                // Tables and columns are loaded separately.
                // In rare cases, a new table may be created between these requests,
                // ... so the columns from the table are loaded but table not.
                // This conditions prevents error.
                if (isset($tableBuilders[$tableId])) {
                    $columnId = $tableId . '.' . $data['COLUMN_NAME'];
                    $columnBuilder = $tableBuilders[$tableId]->addColumn();
                    $columnBuilders[$columnId] = $columnBuilder;
                    $autoIncrement = isset($autoIncrements[$tableId]) ? (int) $autoIncrements[$tableId] : null;
                    $this->processColumnData($columnBuilder, $data, $autoIncrement);
                }
            }

            foreach ($this->queryConstraints($whitelist) as $data) {
                $columnId = $data['TABLE_SCHEMA'] . '.' . $data['TABLE_NAME'] . '.' . $data['COLUMN_NAME'];
                // Column data may not be available with limited database permissions
                if (isset($columnBuilders[$columnId])) {
                    $this->processConstraintData($columnBuilders[$columnId], $data);
                }
            }
        }

        return $builder->build();
    }

    private function processTableData(TableBuilder $builder, array $data): void
    {
        $builder
            ->setName($data['TABLE_NAME'])
            ->setDescription($data['TABLE_COMMENT'] ?? null)
            ->setSchema($data['TABLE_SCHEMA'])
            ->setType($data['TABLE_TYPE'])
            ->setRowCount((int) $data['TABLE_ROWS']);
    }

    private function processColumnData(ColumnBuilder $builder, array $data, ?int $autoIncrement): void
    {
        // Basic values
        $builder
            ->setName($data['COLUMN_NAME'])
            ->setDescription($data['COLUMN_COMMENT'])
            ->setOrdinalPosition((int) $data['ORDINAL_POSITION'])
            ->setType($data['DATA_TYPE'])
            ->setPrimaryKey($data['COLUMN_KEY'] === 'PRI')
            ->setUniqueKey($data['COLUMN_KEY'] === 'UNI')
            ->setNullable($data['IS_NULLABLE'] === 'YES');

        // Default value, if IS_NULLABLE is NO and COLUMN_DEFAULT is null => it means no default value
        if ($data['IS_NULLABLE'] === 'YES' || $data['COLUMN_DEFAULT'] !== null) {
            $builder->setDefault($data['COLUMN_DEFAULT']);
        }

        // Length
        $builder->setLength(
            $data['CHARACTER_MAXIMUM_LENGTH'] ?:
                (
                $data['NUMERIC_SCALE'] > 0 ?
                    $data['NUMERIC_PRECISION'] . ',' . $data['NUMERIC_SCALE'] :
                    $data['NUMERIC_PRECISION'] ?? ''
                )
        );

        // Auto increment
        if ($data['EXTRA'] === 'auto_increment' && $autoIncrement !== null) {
            $builder->setAutoIncrementValue($autoIncrement);
        }
    }

    private function processConstraintData(ColumnBuilder $builder, array $data): void
    {
        // Foreign key
        if (isset($data['REFERENCED_TABLE_NAME'])) {
            try {
                $builder
                    ->addForeignKey()
                    ->setName($data['CONSTRAINT_NAME'])
                    ->setRefSchema($data['REFERENCED_TABLE_SCHEMA'])
                    ->setRefTable($data['REFERENCED_TABLE_NAME'])
                    ->setRefColumn($data['REFERENCED_COLUMN_NAME']);
            } catch (InvalidStateException $e) {
                // In MySQL, one column can have multiple foreign keys, it is useless, but possible.
                // Ignore second foreign key, metadata and manifest expect max one FK.
            }
        }

        // Constraints
        if (isset($data['CONSTRAINT_NAME'])) {
            $builder->addConstraint($data['CONSTRAINT_NAME']);
        }
    }

    /**
     * @param array|InputTable[] $whitelist
     */
    private function queryTables(array $whitelist = []): iterable
    {
        // Build query, REF: https://dev.mysql.com/doc/refman/8.0/en/tables-table.html
        $sql = [];
        $sql[] = 'SELECT *';
        $sql[] = 'FROM INFORMATION_SCHEMA.TABLES as c';

        $this->addTableSchemaWhereConditions($sql, $whitelist);

        $sql[] = 'ORDER BY TABLE_SCHEMA, TABLE_NAME';

        // Run query
        return $this->queryAndFetchAll(implode(' ', $sql));
    }

    /**
     * @param array|InputTable[] $whitelist
     */
    private function queryColumns(?array $whitelist = null): iterable
    {
        // Build query, REF: https://dev.mysql.com/doc/refman/5.1/en/columns-table.html
        $sql = [];
        $sql[] = 'SELECT *';
        $sql[] = 'FROM INFORMATION_SCHEMA.COLUMNS as c';

        $this->addTableSchemaWhereConditions($sql, $whitelist);

        $sql[] = 'ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION';

        // Run query
        return $this->queryAndFetchAll(implode(' ', $sql));
    }

    /**
     * @param array|InputTable[] $whitelist
     */
    private function queryConstraints(?array $whitelist = null): iterable
    {
        // Build query, REF: https://dev.mysql.com/doc/refman/8.0/en/key-column-usage-table.html
        $sql = [];
        $sql[] = 'SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME,';
        $sql[] = 'CONSTRAINT_NAME, REFERENCED_TABLE_NAME,';
        $sql[] = 'REFERENCED_COLUMN_NAME, REFERENCED_TABLE_SCHEMA';
        $sql[] = 'FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE as c';

        $this->addTableSchemaWhereConditions($sql, $whitelist);

        // Run query
        return $this->queryAndFetchAll(implode(' ', $sql));
    }

    private function addTableSchemaWhereConditions(array &$sql, ?array $whitelist): void
    {
        if ($this->database !== null) {
            $sql[] = sprintf(
                'WHERE LOWER(c.TABLE_SCHEMA) = %s',
                $this->db->quote(mb_strtolower($this->database))
            );
        } else {
            $sql[] = 'WHERE c.TABLE_SCHEMA NOT IN ("performance_schema", "mysql", "information_schema", "sys")';
        }

        if ($whitelist) {
            $sql[] =sprintf('AND LOWER(c.TABLE_NAME) IN (%s)', $this->quoteTables($whitelist));
        }
    }

    private function queryAndFetchAll(string $sql): iterable
    {
        /** @var PDOStatement $result */
        $result = $this->db->query($sql);
        while ($row = $result->fetch(PDO::FETCH_ASSOC)) {
            yield $row;
        }
    }

    private function quoteTables(array $whitelist): string
    {
        return implode(
            ', ',
            array_map(function (InputTable $table) {
                if ($this->database && mb_strtolower($table->getSchema()) !== mb_strtolower($this->database)) {
                    throw new InvalidArgumentException(sprintf(
                        'Table "%s"."%s" is not from used database.',
                        $table->getSchema(),
                        $table->getName()
                    ));
                }

                return $this->db->quote(mb_strtolower($table->getName()));
            }, $whitelist)
        );
    }
}
