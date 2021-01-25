<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests\Tables;

use Keboola\DbExtractor\FunctionalTests\TestConnection;
use Keboola\DbExtractor\TraitTests\AddConstraintTrait;
use Keboola\DbExtractor\TraitTests\CreateTableTrait;
use Keboola\DbExtractor\TraitTests\InsertRowsTrait;

trait AutoIncrementTableTrait
{
    use CreateTableTrait;
    use InsertRowsTrait;
    use AddConstraintTrait;

    protected TestConnection $connection;

    public function createAITable(string $name = 'auto Increment Timestamp'): void
    {
        $this->createTable($name, $this->getAIColumns());
    }

    public function generateAIRows(string $tableName = 'auto Increment Timestamp'): void
    {
        $data = $this->getAIRows();
        $this->insertRows($tableName, $data['columns'], $data['data']);
    }

    public function addAIConstraint(string $tableName = 'auto Increment Timestamp'): void
    {
        $this->addConstraint($tableName, 'UNI_KEY_1', 'UNIQUE', '"Weir%d Na-me"');
    }

    private function getAIRows(): array
    {
        return [
            'columns' => ['_Weir%d I-D', 'Weir%d Na-me', 'type', 'someInteger', 'someDecimal', 'date'],
            'data' => [
                [1, 'mario', 'plumber', 1, 1.1, 'TO_DATE(\'2021-01-05\', \'yyyy-mm-dd\')'],
                [2, 'luigi', 'plumber', 2, 2.2, 'TO_DATE(\'2021-01-06\', \'yyyy-mm-dd\')'],
                [3, 'toad', 'mushroom', 3, 3.3, 'TO_DATE(\'2021-01-07\', \'yyyy-mm-dd\')'],
                [4, 'princess', 'royalty', 4, 4.4, 'TO_DATE(\'2021-01-08\', \'yyyy-mm-dd\')'],
                [5, 'wario', 'badguy', 5, 5.5, 'TO_DATE(\'2021-01-09\', \'yyyy-mm-dd\')'],
                [6, 'yoshi', 'horse?', 6, 6.6, 'TO_DATE(\'2021-01-10\', \'yyyy-mm-dd\')'],
            ],
        ];
    }

    private function getAIColumns(): array
    {
        return [
            '_Weir%d I-D' => 'INT NOT NULL',
            'Weir%d Na-me' => 'NVARCHAR2(55) NOT NULL',
            'someInteger' => 'INT',
            'someDecimal' => 'DECIMAL(10,2)',
            'type' => 'NVARCHAR2(55) NULL',
            'date' => 'DATE',
        ];
    }
}
