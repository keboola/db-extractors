<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests;

use Keboola\DbExtractor\FunctionalTests\TestConnection;

trait RemoveAllTablesTrait
{
    use QuoteIdentifierTrait;

    protected TestConnection $connection;

    protected function removeAllTables(): void
    {
        $sql = <<<SQL
BEGIN
  FOR i IN (SELECT ut.table_name FROM USER_TABLES ut) LOOP
    EXECUTE IMMEDIATE 'drop table "'|| i.table_name ||'" CASCADE CONSTRAINTS ';
  END LOOP;
END;
SQL;

        $this->connection->exec($sql);
    }
}
