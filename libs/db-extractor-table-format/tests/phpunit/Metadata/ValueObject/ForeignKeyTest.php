<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TableResultFormat\Tests\Metadata\ValueObject;

use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\ForeignKey;

class ForeignKeyTest extends BaseValueObjectTest
{
    public function createValueObjectFromArray(array $properties): ValueObject
    {
        $name = $properties['name'];
        $refSchema = $properties['refSchema'];
        $refTable = $properties['refTable'];
        $refColumn = $properties['refColumn'];
        return new ForeignKey(
            $name,
            $refSchema,
            $refTable,
            $refColumn,
        );
    }

    public function getAllProperties(): array
    {
        return [
            'name',
            'refSchema',
            'refTable',
            'refColumn',
        ];
    }

    public function getNullableProperties(): array
    {
        return [
            // These properties CAN be set to null in Builder
            // NONE
            // These properties CANNOT be set to null in Builder
            // ... null value in constructor means "not set"
            'name' => self::NULL_MEANS_NOT_SET,
            'refSchema' => self::NULL_MEANS_NOT_SET,
        ];
    }

    public function getEmptyStringNotAllowedProperties(): array
    {
        return [
            'name',
            'refSchema',
            'refTable',
            'refColumn',
        ];
    }

    public function getHasCallbacks(): array
    {
        return [
            'name' => function (ForeignKey $column) {
                return $column->hasName();
            },
            'refSchema' => function (ForeignKey $column) {
                return $column->hasRefSchema();
            },
        ];
    }

    public function getGetCallbacks(): array
    {
        return [
            'name' => function (ForeignKey $column) {
                return $column->getName();
            },
            'refSchema' => function (ForeignKey $column) {
                return $column->getRefSchema();
            },
            'refTable' => function (ForeignKey $column) {
                return $column->getRefTable();
            },
            'refColumn' => function (ForeignKey $column) {
                return $column->getRefColumn();
            },
        ];
    }

    public function getValidInputs(): array
    {
        return [
            [
                'name' => 'name',
                'refSchema' => 'schema',
                'refTable' => 'table',
                'refColumn' => 'column',
            ],
        ];
    }
}
