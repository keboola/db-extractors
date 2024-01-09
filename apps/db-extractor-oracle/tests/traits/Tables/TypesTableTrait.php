<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests\Tables;

use Keboola\DbExtractor\TraitTests\AddConstraintTrait;
use Keboola\DbExtractor\TraitTests\CreateTableTrait;
use Keboola\DbExtractor\TraitTests\InsertRowsTrait;

trait TypesTableTrait
{
    use CreateTableTrait;
    use InsertRowsTrait;

    public function createTypesTable(string $name = 'types'): void
    {
        $this->createTable($name, $this->getTypesColumns());
    }

    public function generateTypesRows(string $tableName = 'types'): void
    {
        $data = $this->getTypesRows();
        $this->insertRows($tableName, $data['columns'], $data['data']);
    }

    private function getTypesRows(): array
    {
        // phpcs:disable Generic.Files.LineLength
        return [
            'columns' => ['varchar','decimal','timestamp','timestamp_length'],
            'data' => [
                [
                    'varchar_data',
                    '12.34',
                    "TO_TIMESTAMP('2021-10-18 23:01:50.000000', 'YYYY-MM-DD HH24:MI:SS.FF6')",
                    "TO_TIMESTAMP('2021-10-18 23:01:50.000000', 'YYYY-MM-DD HH24:MI:SS.FF6')",
                ],
            ],
        ];
        // phpcs:enable
    }

    private function getTypesColumns(): array
    {
        return [
            'varchar' => 'NVARCHAR2 (400)',
            'decimal' => 'DECIMAL(4,2)',
            'timestamp' => 'TIMESTAMP',
            'timestamp_length' => 'TIMESTAMP(6)',
        ];
    }
}
