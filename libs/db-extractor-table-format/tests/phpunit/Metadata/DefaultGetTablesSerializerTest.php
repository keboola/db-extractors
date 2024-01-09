<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TableResultFormat\Tests\Metadata;

use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\MetadataBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\GetTables\DefaultGetTablesSerializer;
use Keboola\DbExtractor\TableResultFormat\Metadata\GetTables\GetTablesSerializer;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

class DefaultGetTablesSerializerTest extends TestCase
{
    private GetTablesSerializer $serializer;

    protected function setUp(): void
    {
        parent::setUp();
        $this->serializer = new DefaultGetTablesSerializer();
    }

    public function testEmpty(): void
    {
        $builder = MetadataBuilder::create();
        $tables = $builder->build();
        $serialized = $this->serializer->serialize($tables);
        Assert::assertSame([], $serialized);
    }

    public function testMinimal(): void
    {
        $builder = MetadataBuilder::create();

        $builder
            ->addTable()
            ->setName('table1')
            ->setSchema('schema1')
            ->addColumn()
            ->setName('column1')
            ->setType('INTEGER');

        $tables = $builder->build();

        $serialized = $this->serializer->serialize($tables);
        Assert::assertSame([
            [
                'name' => 'table1',
                'schema' => 'schema1',
                'columns' => [
                    [
                        'name' => 'column1',
                        'type' => 'INTEGER',
                        'primaryKey' => false,
                    ],
                ],
            ],
        ], $serialized);
    }

    public function testComplex(): void
    {
        $builder = MetadataBuilder::create();

        $table1 = $builder
            ->addTable()
            ->setName('table1')
            ->setSchema('schema1')
            ->setCatalog('catalog1')
            ->setType('TABLE')
            ->setDescription('table 1 description');

        $table1
            ->addColumn()
            ->setOrdinalPosition(0)
            ->setName('column1')
            ->setType('INTEGER')
            ->setPrimaryKey(true)
            ->setUniqueKey(false)
            ->setNullable(false)
            ->setAutoIncrementValue(123);

        $table1
            ->addColumn()
            ->setOrdinalPosition(0)
            ->setName('column2')
            ->setType('VARCHAR')
            ->setLength('255')
            ->setPrimaryKey(false)
            ->setUniqueKey(true)
            ->setNullable(true)
            ->setDefault('text123');

        $builder
            ->addTable()
            ->setName('table2')
            ->setSchema('schema2')
            ->setCatalog('catalog1')
            ->setType('VIEW')
            ->addColumn()
            ->setName('column3')
            ->setType('INTEGER');

        $tables = $builder->build();

        $serialized = $this->serializer->serialize($tables);
        Assert::assertSame([
            [
                'name' => 'table1',
                'schema' => 'schema1',
                'columns' =>
                    [
                        [
                            'name' => 'column1',
                            'type' => 'INTEGER',
                            'primaryKey' => true,
                        ],
                        [
                            'name' => 'column2',
                            'type' => 'VARCHAR',
                            'primaryKey' => false,
                        ],
                    ],
            ],
            [
                'name' => 'table2',
                'schema' => 'schema2',
                'columns' =>
                    [
                        [
                            'name' => 'column3',
                            'type' => 'INTEGER',
                            'primaryKey' => false,
                        ],
                    ],
            ],
        ], $serialized);
    }
}
