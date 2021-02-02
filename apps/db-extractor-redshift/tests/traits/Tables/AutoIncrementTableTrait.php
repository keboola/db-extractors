<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests\Tables;

use Keboola\DbExtractor\TraitTests\AddConstraintTrait;
use Keboola\DbExtractor\TraitTests\CreateTableTrait;
use Keboola\DbExtractor\TraitTests\InsertRowsTrait;

trait AutoIncrementTableTrait
{
    use CreateTableTrait;
    use InsertRowsTrait;
    use AddConstraintTrait;

    public function createAITable(string $name = 'auto increment timestamp'): void
    {
        $this->createTable($name, $this->getAIColumns());
    }

    public function generateAIRows(string $tableName = 'auto increment timestamp'): void
    {
        $data = $this->getAIRows();
        $this->insertRows($tableName, $data['columns'], $data['data']);
    }

    public function addAIConstraint(string $tableName = 'auto increment timestamp'): void
    {
        $this->addConstraint($tableName, 'UNI_KEY_1', 'UNIQUE', '"weir%d na-me"');
    }

    private function getAIRows(): array
    {
        return [
            'columns' => ['_weir%d i-d', 'weir%d na-me', 'type', 'someinteger', 'somedecimal', 'datetime'],
            'data' => [
                [1, 'mario', 'plumber', 1, 1.1, '2021-01-05 13:43:17'],
                [2, 'luigi', 'plumber', 2, 2.2, '2021-01-05 13:43:17'],
                [3, 'toad', 'mushroom', 3, 3.3, '2021-01-05 13:43:17'],
                [4, 'princess', 'royalty', 4, 4.4, '2021-01-05 13:43:17'],
                [5, 'wario', 'badguy', 5, 5.5, '2021-01-05 13:43:17'],
                [6, 'yoshi', 'horse?', 6, 6.6, '2021-01-05 13:43:27'],
            ],
        ];
    }

    private function getAIColumns(): array
    {
        return [
            '_weir%d i-d' => 'INT NOT NULL',
            'weir%d na-me' => 'VARCHAR(30) NOT NULL DEFAULT \'pam\'',
            'someinteger' => 'INT DEFAULT 1',
            'somedecimal' => 'DECIMAL(10,8) DEFAULT 10.2',
            'type' => 'VARCHAR(55) NULL',
            'datetime' => 'DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP',
        ];
    }
}
