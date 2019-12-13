<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Exception\UserException;
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
                    'nullable' => false,
                    'autoIncrement' => false,
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
}
