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
        $sql = <<<SQL
          SET FOREIGN_KEY_CHECKS = 0; 
          SET @tables = NULL;
          SET GROUP_CONCAT_MAX_LEN=131071;
        
          SELECT GROUP_CONCAT('`', table_schema, '`.`', table_name, '`') INTO @tables
          FROM   information_schema.tables 
          WHERE  TABLE_SCHEMA NOT IN ("performance_schema", "mysql", "information_schema", "sys");
          SELECT IFNULL(@tables, '') INTO @tables;
        
          SET        @tables = CONCAT('DROP TABLE IF EXISTS ', @tables);
          PREPARE    stmt FROM @tables;
          EXECUTE    stmt;
          DEALLOCATE PREPARE stmt;
          SET        FOREIGN_KEY_CHECKS = 1;
        SQL;

        $this->connection->query($sql);
    }
}
