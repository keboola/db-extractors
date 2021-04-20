<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TableResultFormat\Tests\Metadata\ValueObject;

use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\MetadataBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\TableBuilder;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

class ColumnCollectionTest extends TestCase
{
    public function testCaseSensitiveSearch(): void
    {
        $builder = TableBuilder::create();
        $builder->setName('testTable');
        $builder
            ->addColumn()
            ->setName('abc')
            ->setType('integer');
        $builder
            ->addColumn()
            ->setName('aBC')
            ->setType('integer');
        $builder
            ->addColumn()
            ->setName('AbC')
            ->setType('integer');
        $builder
            ->addColumn()
            ->setName('aBc')
            ->setType('integer');
        $table = $builder->build();
        $collection = $table->getColumns();

        Assert::assertSame('aBC', $collection->getByName('aBC')->getName());
        Assert::assertSame('aBC', $collection->getBySanitizedName('aBC')->getName());
    }

    public function testCaseInsensitiveSearch(): void
    {
        $builder = TableBuilder::create();
        $builder->setName('testTable');
        $builder
            ->addColumn()
            ->setName('aBC')
            ->setType('integer');
        $table = $builder->build();
        $collection = $table->getColumns();

        Assert::assertSame('aBC', $collection->getByName('abc')->getName());
        Assert::assertSame('aBC', $collection->getBySanitizedName('abc')->getName());
    }

    public function testSortByOrdinalPosition(): void
    {
        $builder = TableBuilder::create();
        $builder->setName('testTable');

        $builder
            ->addColumn()
            ->setName('D')
            ->setType('integer')
            ->setOrdinalPosition(4);

        $builder
            ->addColumn()
            ->setName('B')
            ->setType('integer')
            ->setOrdinalPosition(2);

        $builder
            ->addColumn()
            ->setName('A')
            ->setType('integer')
            ->setOrdinalPosition(1);

        $builder
            ->addColumn()
            ->setName('C')
            ->setType('integer')
            ->setOrdinalPosition(3);

        $table = $builder->build();
        $collection = $table->getColumns();
        Assert::assertSame(['A', 'B', 'C', 'D'], $collection->getNames());
    }

    public function testSortOrdinalPositionNull(): void
    {
        $builder = TableBuilder::create();
        $builder->setName('testTable');

        $builder
            ->addColumn()
            ->setName('A')
            ->setType('integer');

        $builder
            ->addColumn()
            ->setName('B')
            ->setType('integer');

        $builder
            ->addColumn()
            ->setName('C')
            ->setType('integer');

        $builder
            ->addColumn()
            ->setName('D')
            ->setType('integer');

        $table = $builder->build();
        $collection = $table->getColumns();
        Assert::assertSame(['A', 'B', 'C', 'D'], $collection->getNames());
    }

    public function testSortOrdinalPositionNullMultiple(): void
    {
        $builder = TableBuilder::create();
        $builder->setName('testTable');

        $expectedOrder = [];
        for ($i=0; $i<100; $i++) {
            $name = 'COL' . $i;
            $expectedOrder[] = $name;
            $builder
                ->addColumn()
                ->setName($name)
                ->setType('integer');
        }

        $table = $builder->build();
        $collection = $table->getColumns();
        Assert::assertSame($expectedOrder, $collection->getNames());
    }
}
