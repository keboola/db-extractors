<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests\Tables;

use Keboola\DbExtractor\TraitTests\AddConstraintTrait;
use Keboola\DbExtractor\TraitTests\CreateTableTrait;
use Keboola\DbExtractor\TraitTests\InsertRowsTrait;

trait SalesLateBindViewTrait
{
    use SalesTableTrait;

    public function createSalesLateBindView(string $name = 'sales', array $columnsInView = ['*']): void
    {
        $this->createLateBindView($name, $this->getSalesColumns(), $columnsInView);
    }
}
