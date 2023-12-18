<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TableResultFormat\Tests\Metadata\ValueObject;

use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\MetadataBuilder;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

class TableCollectionTest extends TestCase
{
    public function testCaseSensitiveSearch(): void
    {
        $builder = MetadataBuilder::create();
        $builder
            ->addTable()
            ->setSchema('test')
            ->setName('abc')
            ->setColumnsNotExpected();
        $builder
            ->addTable()
            ->setSchema('teST')
            ->setName('aBC')
            ->setColumnsNotExpected();
        $builder
            ->addTable()
            ->setSchema('TesT')
            ->setName('AbC')
            ->setColumnsNotExpected();
        $builder
            ->addTable()
            ->setSchema('teSt')
            ->setName('aBc')
            ->setColumnsNotExpected();
        $collection = $builder->build();

        $table = $collection->getByNameAndSchema('aBC', 'teST');
        Assert::assertSame('teST', $table->getSchema());
        Assert::assertSame('aBC', $table->getName());
    }

    public function testCaseInsensitiveSearch(): void
    {
        $builder = MetadataBuilder::create();
        $builder
            ->addTable()
            ->setSchema('teST')
            ->setName('aBC')
            ->setColumnsNotExpected();
        $collection = $builder->build();

        $table = $collection->getByNameAndSchema('abc', 'test');
        Assert::assertSame('teST', $table->getSchema());
        Assert::assertSame('aBC', $table->getName());
    }
}
