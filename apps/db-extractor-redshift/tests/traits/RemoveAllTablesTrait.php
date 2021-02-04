<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests;

use PDO;

trait RemoveAllTablesTrait
{
    use QuoteIdentifierTrait;

    protected PDO $connection;

    protected function removeAllTables(): void
    {
        $tablesSql = <<<SQL
SELECT * FROM information_schema.tables
WHERE table_schema != 'pg_catalog' AND table_schema != 'information_schema' AND table_schema != 'pg_internal';
SQL;

        foreach ((array) $this->connection->query($tablesSql)->fetchAll(PDO::FETCH_ASSOC) as $item) {
            $this->connection->exec(sprintf(
                'DROP TABLE IF EXISTS "%s"."%s";',
                $item['table_schema'],
                $item['table_name']
            ));
        }
    }
}
