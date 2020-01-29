<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\TableResultFormat\ForeignKey;
use Keboola\DbExtractor\TableResultFormat\Table;
use Keboola\DbExtractor\TableResultFormat\TableColumn;
use PHPUnit\Framework\TestCase;

class TableResultFormatTest extends TestCase
{
    public function testTableFormat(): void
    {
        $table = new Table();
        $table
            ->setName('testName')
            ->setCatalog('catalog')
            ->setSchema('schema')
            ->setType('view')
            ->setRowCount(22);

        $column = new TableColumn();
        $column
            ->setName('Asdno osdn')
            ->setType('varchar');

        $table->addColumn($column);

        $this->assertEquals([
            'name' => 'testName',
            'catalog' => 'catalog',
            'schema' => 'schema',
            'type' => 'view',
            'rowCount' => 22,
            'columns' => [
                0 => [
                    'name' => 'Asdno osdn',
                    'sanitizedName' => 'Asdno_osdn',
                    'type' => 'varchar',
                    'primaryKey' => false,
                    'uniqueKey' => false,
                ],
            ],
        ], $table->getOutput());
    }

    public function testTableFormatWithForeignKey(): void
    {
        $table = new Table();
        $table
            ->setName('testName')
            ->setCatalog('catalog')
            ->setSchema('schema')
            ->setType('view')
            ->setRowCount(22);

        $column = new TableColumn();
        $column
            ->setName('Asdno osdn')
            ->setType('varchar');

        $foreignKey = new ForeignKey();
        $foreignKey
            ->setName('testForeignKey')
            ->setRefSchema('refSchema')
            ->setRefTable('refTable')
            ->setRefColumn('refColumn');

        $column->setForeignKey($foreignKey);

        $table->addColumn($column);

        $this->assertEquals([
            'name' => 'testName',
            'catalog' => 'catalog',
            'schema' => 'schema',
            'type' => 'view',
            'rowCount' => 22,
            'columns' => [
                0 => [
                    'name' => 'Asdno osdn',
                    'sanitizedName' => 'Asdno_osdn',
                    'type' => 'varchar',
                    'primaryKey' => false,
                    'uniqueKey' => false,
                    'foreignKey' => true,
                    'foreignKeyName' => 'testForeignKey',
                    'foreignKeyRefSchema' => 'refSchema',
                    'foreignKeyRefTable' => 'refTable',
                    'foreignKeyRefColumn' => 'refColumn',
                ],
            ],
        ], $table->getOutput());
    }

    public function testInvalidTable(): void
    {
        $table = new Table();
        $table
            ->setCatalog('catalog');

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Parameter \'name\' is required');
        $table->getOutput();
    }

    public function testInvalidTableColumn(): void
    {
        $table = new Table();
        $table
            ->setName('testInvalitTableColumn')
            ->setCatalog('catalog');

        $columns = [
            [
                'name' => 'Asdno osdn',
                'sanitizedName' => 'Asdno_osdn',
                'type' => 'varchar',
                'primaryKey' => false,
                'uniqueKey' => false,
            ],
        ];

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Column is not instance \Keboola\DbExtractor\TableResultFormat\TableColumn');
        $table->setColumns($columns);
    }
}
