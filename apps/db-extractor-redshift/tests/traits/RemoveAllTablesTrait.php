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
WHERE table_schema != 'pg_catalog' AND table_schema != 'information_schema' AND table_schema != 'pg_internal'
ORDER BY table_type='VIEW' DESC, table_type='BASE TABLE' DESC;
SQL;

        foreach ((array) $this->connection->query($tablesSql)->fetchAll(PDO::FETCH_ASSOC) as $item) {
            $drop = 'TABLE';
            if ($item['table_type'] === 'VIEW') {
                $drop = 'VIEW';
            }

            $this->connection->exec(sprintf(
                'DROP %s IF EXISTS "%s"."%s";',
                $drop,
                $item['table_schema'],
                $item['table_name'],
            ));
        }
    }
}
