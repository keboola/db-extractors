<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests;

use PDO;

trait CreateTableTrait
{
    use QuoteIdentifierTrait;

    protected Pdo $connection;

    public function createTable(string $tableName, array $columns): void
    {
        // Generate columns statement
        $columnsSql = [];
        foreach ($columns as $name => $sqlDef) {
            $columnsSql[] = $this->quoteIdentifier($name) . ' ' . $sqlDef;
        }

        // Create table
        $this->connection->prepare(sprintf(
            'CREATE TABLE %s.%s (%s)',
            $this->quoteIdentifier((string) getenv('REDSHIFT_DB_SCHEMA')),
            $this->quoteIdentifier($tableName),
            implode(', ', $columnsSql)
        ))->execute();
    }

    public function createLateBindView(string $tableName, array $columns, array $columnsInView): void
    {
        $this->createTable($tableName, $columns);

        $this->connection->prepare(sprintf(
            'CREATE VIEW %s AS SELECT %s FROM %s.%s WITH NO SCHEMA BINDING;',
            $this->quoteIdentifier($tableName . '_view'),
            implode(', ', $columnsInView),
            $this->quoteIdentifier((string) getenv('REDSHIFT_DB_SCHEMA')),
            $this->quoteIdentifier($tableName)
        ))->execute();
    }
}
