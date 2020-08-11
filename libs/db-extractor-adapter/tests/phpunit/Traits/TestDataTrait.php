<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\Tests\Traits;

use PDO;

trait TestDataTrait
{
    protected PDO $connection;

    protected function createTestConnection(): PDO
    {
        $dns = sprintf(
            'mysql:host=%s;port=%s;dbname=%s;charset=utf8',
            $host ?? getenv('DB_HOST'),
            $port ?? getenv('DB_PORT'),
            getenv('DB_DATABASE'),
        );
        return new PDO($dns, (string) getenv('DB_USER'), (string) getenv('DB_PASSWORD'));
    }

    protected function createTownsTable(): void
    {
        $sql = <<<END
          CREATE TABLE `towns` (
            `id` int NOT NULL AUTO_INCREMENT,
            `name` varchar(255) NOT NULL,
            `population` int NOT NULL,
            PRIMARY KEY (id)
          );
        END;

        $this->connection->query($sql);

        $data = [
            [1, 'Praha', 1165581],
            [2, 'Brno', 369559],
            [3, 'Ostrava', 313088],
            [4, 'Plzen', 164180],
            [5, 'Olomouc', 101268],
            [6, 'Liberec', 97770],
        ];

        foreach ($data as $row) {
            $this->connection->query(sprintf(
                'INSERT INTO `towns` VALUES(%d, "%s", %d);',
                $row[0],
                $row[1],
                $row[2],
            ));
        }
    }

    protected function dropAllTables(): void
    {
        $sql = <<<END
          SET FOREIGN_KEY_CHECKS = 0; 
          SET @tables = NULL;
          SET GROUP_CONCAT_MAX_LEN=32768;
        
          SELECT GROUP_CONCAT('`', table_schema, '`.`', table_name, '`') INTO @tables
          FROM   information_schema.tables 
          WHERE  TABLE_SCHEMA NOT IN ("performance_schema", "mysql", "information_schema", "sys");
          SELECT IFNULL(@tables, '') INTO @tables;
        
          SET        @tables = CONCAT('DROP TABLE IF EXISTS ', @tables);
          PREPARE    stmt FROM @tables;
          EXECUTE    stmt;
          DEALLOCATE PREPARE stmt;
          SET        FOREIGN_KEY_CHECKS = 1;
        END;

        $this->connection->query($sql);
    }
}
