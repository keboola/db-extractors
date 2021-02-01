<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests;

use Keboola\DbExtractor\FunctionalTests\TestConnection;

trait CreateTableTrait
{
    use QuoteIdentifierTrait;

    protected TestConnection $connection;

    public function createTable(string $tableName, array $columns): void
    {
        // Generate columns statement
        $columnsSql = [];
        foreach ($columns as $name => $sqlDef) {
            $columnsSql[] = $this->quoteIdentifier($name) . ' ' . $sqlDef;
        }

        $sql = sprintf(
            'CREATE TABLE %s (%s) tablespace users',
            $this->quoteIdentifier($tableName),
            implode(', ', $columnsSql)
        );

        // Create table
        $this->connection->exec($sql);
    }
}
