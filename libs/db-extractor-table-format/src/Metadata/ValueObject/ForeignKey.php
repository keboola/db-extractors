<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject;

use Keboola\DbExtractor\TableResultFormat\Exception\InvalidArgumentException;
use Keboola\DbExtractor\TableResultFormat\Exception\PropertyNotSetException;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject;

class ForeignKey implements ValueObject
{
    private ?string $name;

    private ?string $refSchema;

    private string $refTable;

    private string $refColumn;

    /**
     * @internal Should be created using ForeignKeyBuilder, don't call it directly!
     */
    public function __construct(?string $name, ?string $refSchema, string $refTable, string $refColumn)
    {
        if ($name !== null && $name === '') {
            throw new InvalidArgumentException('Name of foreign key cannot be empty string.');
        }

        if ($refSchema !== null && $refSchema === '') {
            throw new InvalidArgumentException('Ref schema of foreign key cannot be empty string.');
        }

        if ($refTable === '') {
            throw new InvalidArgumentException('Ref table cannot be empty.');
        }

        if ($refColumn === '') {
            throw new InvalidArgumentException('Ref column cannot be empty.');
        }

        $this->name = $name;
        $this->refSchema = $refSchema;
        $this->refTable = $refTable;
        $this->refColumn = $refColumn;
    }

    public function hasName(): bool
    {
        return $this->name !== null;
    }

    public function getName(): string
    {
        if ($this->name === null) {
            throw new PropertyNotSetException('Name is not set.');
        }

        return $this->name;
    }

    public function hasRefSchema(): bool
    {
        return $this->refSchema !== null;
    }

    public function getRefSchema(): string
    {
        if ($this->refSchema === null) {
            throw new PropertyNotSetException('RefSchema is not set.');
        }

        return $this->refSchema;
    }

    public function getRefTable(): string
    {
        return $this->refTable;
    }

    public function getRefColumn(): string
    {
        return $this->refColumn;
    }
}
