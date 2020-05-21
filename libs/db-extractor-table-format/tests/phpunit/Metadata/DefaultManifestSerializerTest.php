<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TableResultFormat\Tests\Metadata;

use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\ColumnBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\TableBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\Manifest\DefaultManifestSerializer;
use Keboola\DbExtractor\TableResultFormat\Metadata\Manifest\ManifestSerializer;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

class DefaultManifestSerializerTest extends TestCase
{
    private ManifestSerializer $serializer;

    protected function setUp(): void
    {
        parent::setUp();
        $this->serializer = new DefaultManifestSerializer();
    }

    public function testTableMinimal(): void
    {
        $tableBuilder = TableBuilder::create()
            ->setName('simple')
            ->setSchema('testdb');

        $tableBuilder
            ->addColumn()
            ->setName('Col1')
            ->setType('INT');

        $table = $tableBuilder->build();

        $expectedOutput = [
            [
                'key' => 'KBC.name',
                'value' => 'simple',
            ],
            [
                'key' => 'KBC.sanitizedName',
                'value' => 'simple',
            ],
            [
                'key' => 'KBC.schema',
                'value' => 'testdb',
            ],
        ];
        Assert::assertEquals($expectedOutput, $this->serializer->serializeTable($table));
    }

    public function testTableComplex(): void
    {
        $tableBuilder = TableBuilder::create()
            ->setName('simple')
            ->setSchema('testdb')
            ->setType('BASE TABLE')
            ->setRowCount(2);

        $tableBuilder
            ->addColumn()
            ->setName('Col1')
            ->setType('INT');

        $table = $tableBuilder->build();

        $expectedOutput = [
            [
                'key' => 'KBC.name',
                'value' => 'simple',
            ],
            [
                'key' => 'KBC.sanitizedName',
                'value' => 'simple',
            ],
            [
                'key' => 'KBC.schema',
                'value' => 'testdb',
            ],
            [
                'key' => 'KBC.type',
                'value' => 'BASE TABLE',
            ],
            [
                'key' => 'KBC.rowCount',
                'value' => '2',
            ],
        ];
        Assert::assertEquals($expectedOutput, $this->serializer->serializeTable($table));
    }

    public function testColumnMinimal(): void
    {
        $column = ColumnBuilder::create()
            ->setName('simple')
            ->setType('varchar')
            ->build();

        $expectedOutput = [
            [
                'key' => 'KBC.datatype.type',
                'value' => 'varchar',
            ],
            // Keboola\Datatype\Definition\Common has default value for "nullable" set to "true"
            [
                'key' => 'KBC.datatype.nullable',
                'value' => true,
            ],
            [
                'key' => 'KBC.datatype.basetype',
                'value' => 'STRING',
            ],
            [
                'key' => 'KBC.sourceName',
                'value' => 'simple',
            ],
            [
                'key' => 'KBC.sanitizedName',
                'value' => 'simple',
            ],
            [
                'key' => 'KBC.primaryKey',
                'value' => false,
            ],
        ];

        Assert::assertEquals($expectedOutput, $this->serializer->serializeColumn($column));
    }

    public function testColumnComplex(): void
    {
        $column = ColumnBuilder::create()
            ->setName('_weird-I-d')
            ->setType('varchar')
            ->setPrimaryKey(true)
            ->setLength('155')
            ->setNullable(false)
            ->setDefault('abc')
            ->setOrdinalPosition(1)
            ->addConstraint('abc')
            ->addConstraint('def')
            ->build();

        $expectedOutput = [
            [
                'key' => 'KBC.datatype.type',
                'value' => 'varchar',
            ],
            [
                'key' => 'KBC.datatype.nullable',
                'value' => false,
            ],
            [
                'key' => 'KBC.datatype.basetype',
                'value' => 'STRING',
            ],
            [
                'key' => 'KBC.datatype.length',
                'value' => '155',
            ],
            [
                'key' => 'KBC.datatype.default',
                'value' => 'abc',
            ],
            [
                'key' => 'KBC.sourceName',
                'value' => '_weird-I-d',
            ],
            [
                'key' => 'KBC.sanitizedName',
                'value' => 'weird_I_d',
            ],
            [
                'key' => 'KBC.primaryKey',
                'value' => true,
            ],
            [
                'key' => 'KBC.ordinalPosition',
                'value' => '1',
            ],
            [
                'key' => 'KBC.constraintName',
                'value' => 'abc',
            ],
            [
                'key' => 'KBC.constraintName',
                'value' => 'def',
            ],
        ];

        Assert::assertEquals($expectedOutput, $this->serializer->serializeColumn($column));
    }
}
