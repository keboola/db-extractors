<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TableResultFormat\Tests\Metadata\ValueObject;

use Keboola\DbExtractor\TableResultFormat\Exception\NoColumnException;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\MetadataBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\Table;
use PHPStan\Testing\TestCase;
use PHPUnit\Framework\Assert;

class MetadataTest extends TestCase
{
    public function testEmpty(): void
    {
        $builder = MetadataBuilder::create();
        $tableCollection = $builder->build();
        Assert::assertSame(true, $tableCollection->isEmpty());
        Assert::assertSame(0, $tableCollection->count());
        Assert::assertSame([], iterator_to_array($tableCollection->getIterator()));
    }

    public function testAddTable(): void
    {
        $builder = MetadataBuilder::create();
        $builder
            ->addTable()
            ->setName('Table')
            ->setColumnsNotExpected();

        $tableCollection = $builder->build();
        Assert::assertSame(false, $tableCollection->isEmpty());
        Assert::assertSame(1, $tableCollection->count());

        /** @var Table[] $tables */
        $tables = iterator_to_array($tableCollection->getIterator());
        Assert::assertCount(1, $tables);
        Assert::assertSame('Table', $tables[0]->getName());
    }

    public function testTableWithNoColumnsDefault(): void
    {
        $builder = MetadataBuilder::create();
        $builder
            ->addTable()
            ->setName('Table');
        $tableCollection = $builder->build();

        // Table without columns is ignored
        Assert::assertSame(true, $tableCollection->isEmpty());
    }

    public function testTableWithNoColumnsAllowed(): void
    {
        $builder = MetadataBuilder::create();
        $builder->setIgnoreTableWithoutColumns(true);
        $builder
            ->addTable()
            ->setName('Table');
        $tableCollection = $builder->build();

        // Table without columns is ignored
        Assert::assertSame(true, $tableCollection->isEmpty());
    }

    public function testTableWithNoColumnsNotAllowed(): void
    {
        $builder = MetadataBuilder::create();
        $builder->setIgnoreTableWithoutColumns(false);
        $builder
            ->addTable()
            ->setName('Table');

        $this->expectException(NoColumnException::class);
        $this->expectExceptionMessage('Table "Table" must have at least one column.');
        $tableCollection = $builder->build();
    }
}
