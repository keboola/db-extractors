<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests\Tables;

use Keboola\DbExtractor\TraitTests\AddConstraintTrait;
use Keboola\DbExtractor\TraitTests\CreateTableTrait;
use Keboola\DbExtractor\TraitTests\InsertRowsTrait;

trait SalesTableTrait
{
    use CreateTableTrait;
    use InsertRowsTrait;
    use AddConstraintTrait;

    public function createSalesTable(string $name = 'sales'): void
    {
        $this->createTable($name, $this->getSalesColumns());
    }

    public function generateSalesRows(string $tableName = 'sales'): void
    {
        $data = $this->getSalesRows();
        $this->insertRows($tableName, $data['columns'], $data['data']);
    }

    public function addSalesConstraint(string $tableName = 'sales', array $primaryKey = []): void
    {
        if ($primaryKey) {
            $this->addConstraint(
                $tableName,
                'PK_' . $tableName,
                'PRIMARY KEY',
                implode(', ', $primaryKey),
            );
        }

        if ($tableName === 'sales2') {
            $this->addConstraint(
                $tableName,
                'FK_sales_sales2',
                'FOREIGN KEY',
                'createdat',
                'sales(createdat)',
            );
        }
    }

    private function getSalesRows(): array
    {
        // phpcs:disable Generic.Files.LineLength
        return [
            'columns' => ['usergender','usercity','usersentiment','zipcode','sku','createdat','category','price','county','countycode','userstate','categorygroup'],
            'data' => [
                ['Female','Mize','-1','39153','ZD111318','2013-09-23 22:38:29','Cameras','708','Smith','28129','Mississippi','Electronics'],
                ['Male','The Lakes','1','89124','ZD111402','2013-09-23 22:38:30','Televisions','1546','Clark','32003','Nevada','Electronics'],
                ['Male','Baldwin','1','21020','ZD111483','2013-09-23 22:38:31','Loose Stones','1262','Baltimore','24005','Maryland','Jewelry'],
                ['Female','Archbald','1','18501','ZD111395','2013-09-23 22:38:32','Stereo','104','Lackawanna','42069','Pennsylvania','Electronics'],
                ['Male','Berea','0','44127','ZD111451','2013-09-23 22:38:33','Earings','1007','Cuyahoga','39035','Ohio','Jewelry'],
                ['Male','Baldwin','0','21219','ZD111471','2013-09-23 22:38:34','Jewelry Boxes','103','Baltimore','24005','Maryland','Jewelry'],
                ['Male','Phoenix','1','85083','ZD111228','2013-09-23 22:38:35','Reference','18','Maricopa','04013','Arizona','Books'],
                ['Female','Martinsburg','-1','25428','ZD111340','2013-09-23 22:38:36','Dvd/Vcr Players','197','Berkeley','54003','West Virginia','Electronics'],
                ['Male','Los Angeles','0','91328','ZD111595','2013-09-23 22:38:37','Maternity','67','Los Angeles','06037','California','Women'],
                ['Female','Bolton','1','28455','ZD111577','2013-09-23 22:38:38','Dresses','61','Columbus','37047','North Carolina','Women'],
            ],
        ];
        // phpcs:enable
    }

    private function getSalesColumns(): array
    {
        return [
            'usergender' => 'text NULL',
            'usercity' => 'text NULL',
            'usersentiment' => 'text NULL',
            'zipcode' => 'text NULL',
            'sku' => 'text NULL',
            'createdat' => 'varchar(64) NOT NULL',
            'category' => 'text NULL',
            'price' => 'text NULL',
            'county' => 'text NULL',
            'countycode' => 'text NULL',
            'userstate' => 'text NULL',
            'categorygroup' => 'text NULL',
        ];
    }
}
