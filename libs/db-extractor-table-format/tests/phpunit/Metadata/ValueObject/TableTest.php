<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TableResultFormat\Tests\Metadata\ValueObject;

use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\ColumnBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\ColumnCollection;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\Table;

class TableTest extends BaseValueObjectTest
{
    public function createValueObjectFromArray(array $properties): ValueObject
    {
        $name = $properties['name'];
        $sanitizedName = $properties['sanitizedName'];
        $description = $properties['description'];
        $schema = $properties['schema'];
        $catalog = $properties['catalog'];
        $tablespaceName = $properties['tablespaceName'];
        $owner = $properties['owner'];
        $type = $properties['type'];
        $rowCount = $properties['rowCount'];
        $column = $properties['columns'];
        $cdcEnabled = $properties['cdcEnabled'];
        return new Table(
            $name,
            $sanitizedName,
            $description,
            $schema,
            $catalog,
            $tablespaceName,
            $owner,
            $type,
            $rowCount,
            $column,
            null,
            $cdcEnabled
        );
    }

    public function getAllProperties(): array
    {
        return [
            'name',
            'sanitizedName',
            'description',
            'schema',
            'catalog',
            'tablespaceName',
            'owner',
            'type',
            'rowCount',
            'columns',
            'cdcEnabled',
        ];
    }

    public function getNullableProperties(): array
    {
        return [
            // These properties CAN be set to null in Builder
            'description' => self::NULL_MEANS_NOT_SET,
            'schema' => self::NULL_MEANS_NOT_SET,
            'catalog' => self::NULL_MEANS_NOT_SET,
            'tablespaceName' => self::NULL_MEANS_NOT_SET,
            'owner' => self::NULL_MEANS_NOT_SET,
            'rowCount' => self::NULL_MEANS_NOT_SET,
            // These properties CANNOT be set to null in Builder
            // ... null value in constructor means "not set"
            'type' => self::NULL_MEANS_NOT_SET,
            'columns' => self::NULL_MEANS_NOT_SET,
            'cdcEnabled' => self::NULL_MEANS_NOT_SET,
        ];
    }

    public function getEmptyStringNotAllowedProperties(): array
    {
        return [
            'name',
            'sanitizedName',
        ];
    }

    public function getHasCallbacks(): array
    {
        return [
            'description' => function (Table $column) {
                return  $column->hasDescription();
            },
            'schema' => function (Table $column) {
                return  $column->hasSchema();
            },
            'catalog' => function (Table $column) {
                return  $column->hasCatalog();
            },
            'tablespaceName' => function (Table $column) {
                return  $column->hasTablespaceName();
            },
            'owner' => function (Table $column) {
                return  $column->hasOwner();
            },
            'type' => function (Table $column) {
                return  $column->hasType();
            },
            'rowCount' => function (Table $column) {
                return  $column->hasRowCount();
            },
            'columns' => function (Table $column) {
                return  $column->hasColumns();
            },
            'cdcEnabled' => function (Table $column) {
                return  $column->hasCdcEnabled();
            },
        ];
    }

    public function getGetCallbacks(): array
    {
        return [
            'name' => function (Table $column) {
                return $column->getName();
            },
            'sanitizedName' => function (Table $column) {
                return $column->getSanitizedName();
            },
            'description' => function (Table $column) {
                return $column->getDescription();
            },
            'schema' => function (Table $column) {
                return $column->getSchema();
            },
            'catalog' => function (Table $column) {
                return $column->getCatalog();
            },
            'tablespaceName' => function (Table $column) {
                return $column->getTablespaceName();
            },
            'owner' => function (Table $column) {
                return $column->getOwner();
            },
            'type' => function (Table $column) {
                return $column->getType();
            },
            'rowCount' => function (Table $column) {
                return $column->getRowCount();
            },
            'columns' => function (Table $column) {
                return $column->getColumns();
            },
            'cdcEnabled' => function (Table $column) {
                return $column->getCdcEnabled();
            },
        ];
    }

    public function getValidInputs(): array
    {
        return [
            [
                'name' => 'Table Name',
                'sanitizedName' => 'table_name',
                'description' => 'Some description',
                'schema' => 'my_schema',
                'catalog' => 'my_catalog',
                'tablespaceName' => 'Table Space',
                'owner' => 'My Owner',
                'type' => 'table',
                'rowCount' => 123,
                'cdcEnabled' => true,
                'columns' => new ColumnCollection([
                    ColumnBuilder::create()
                        ->setOrdinalPosition(0)->setName('Col 1')->setType('integer')->build(),
                    ColumnBuilder::create()
                        ->setOrdinalPosition(1)->setName('Col 2')->setType('integer')->build(),
                ]),
            ],
        ];
    }
}
