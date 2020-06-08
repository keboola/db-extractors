<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject;

use Keboola\DbExtractor\TableResultFormat\Exception\InvalidArgumentException;
use Keboola\DbExtractor\TableResultFormat\Exception\PropertyNotSetException;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject;

class Table implements ValueObject
{
    private string $name;

    private string $sanitizedName;

    private ?string $description;

    private ?string $schema;

    private ?string $catalog;

    private ?string $tablespaceName;

    private ?string $owner;

    private ?string $type;

    private ?int $rowCount;

    private ?ColumnCollection $columns;

    /**
     * @internal Should be created using TableBuilder, don't call it directly!
     */
    public function __construct(
        string $name,
        string $sanitizedName,
        ?string $description,
        ?string $schema,
        ?string $catalog,
        ?string $tablespaceName,
        ?string $owner,
        ?string $type,
        ?int $rowCount,
        ?ColumnCollection $columns
    ) {
        if (empty($name)) {
            throw new InvalidArgumentException('Table\'s name cannot be empty.');
        }

        if (empty($sanitizedName)) {
            throw new InvalidArgumentException('Table\'s sanitized name cannot be empty.');
        }

        if ($description !== null && empty($description)) {
            throw new InvalidArgumentException('Table\'s description cannot be empty string, use null.');
        }

        if ($schema !== null && empty($schema)) {
            throw new InvalidArgumentException('Table\'s schema cannot be empty string, use null.');
        }

        if ($catalog !== null && empty($catalog)) {
            throw new InvalidArgumentException('Table\'s catalog cannot be empty string, use null.');
        }

        if ($tablespaceName !== null && empty($tablespaceName)) {
            throw new InvalidArgumentException('Table\'s tablespaceName cannot be empty string, use null.');
        }

        if ($owner !== null && empty($owner)) {
            throw new InvalidArgumentException('Table\'s owner cannot be empty string, use null.');
        }

        if ($type !== null && empty($type)) {
            throw new InvalidArgumentException('Table\'s type cannot be empty string, use null.');
        }

        if ($columns && $columns->isEmpty()) {
            throw new InvalidArgumentException(sprintf('Table "%s" must have at least one column.', $name));
        }

        $this->name = $name;
        $this->sanitizedName = $sanitizedName;
        $this->description = $description;
        $this->schema = $schema;
        $this->catalog = $catalog;
        $this->tablespaceName = $tablespaceName;
        $this->owner = $owner;
        $this->type = $type;
        $this->rowCount = $rowCount;
        $this->columns = $columns;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getSanitizedName(): string
    {
        return $this->sanitizedName;
    }

    public function hasDescription(): bool
    {
        return $this->description !== null;
    }

    public function getDescription(): string
    {
        if ($this->description === null) {
            throw new PropertyNotSetException('Description is not set.');
        }
        return $this->description;
    }

    public function hasSchema(): bool
    {
        return $this->schema !== null;
    }

    public function getSchema(): string
    {
        if ($this->schema === null) {
            throw new PropertyNotSetException('Schema is not set.');
        }
        return $this->schema;
    }

    public function hasCatalog(): bool
    {
        return $this->catalog !== null;
    }

    public function getCatalog(): string
    {
        if ($this->catalog === null) {
            throw new PropertyNotSetException('Catalog is not set.');
        }
        return $this->catalog;
    }

    public function hasTablespaceName(): bool
    {
        return $this->tablespaceName !== null;
    }

    public function getTablespaceName(): string
    {
        if ($this->tablespaceName === null) {
            throw new PropertyNotSetException('TablespaceName is not set.');
        }
        return $this->tablespaceName;
    }

    public function hasOwner(): bool
    {
        return $this->owner !== null;
    }

    public function getOwner(): string
    {
        if ($this->owner === null) {
            throw new PropertyNotSetException('Owner is not set.');
        }
        return $this->owner;
    }

    public function hasType(): bool
    {
        return $this->type !== null;
    }

    public function getType(): string
    {
        if ($this->type === null) {
            throw new PropertyNotSetException('Type is not set.');
        }
        return $this->type;
    }

    public function hasRowCount(): bool
    {
        return $this->rowCount !== null;
    }

    public function getRowCount(): int
    {
        if ($this->rowCount === null) {
            throw new PropertyNotSetException('Row count is not set.');
        }
        return $this->rowCount;
    }

    public function hasColumns(): bool
    {
        return $this->columns !== null;
    }

    public function getColumns(): ColumnCollection
    {
        if ($this->columns === null) {
            throw new PropertyNotSetException('Columns are not set.');
        }
        return $this->columns;
    }
}
