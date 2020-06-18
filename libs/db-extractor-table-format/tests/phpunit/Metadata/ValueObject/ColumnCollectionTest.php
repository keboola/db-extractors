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
}
