<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TableResultFormat\Metadata\Builder;

use Keboola\DbExtractor\TableResultFormat\Exception\InvalidArgumentException;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\ForeignKey;

class ForeignKeyBuilder implements Builder
{
    use RequiredPropertiesTrait;

    /**
     * List of always required properties
     */
    public const ALWAYS_REQUIRED_PROPERTIES = ['refTable', 'refColumn'];

    /**
     * List of properties that can be marked as required through the constructor.
     */
    public const OPTIONAL_REQUIRED_PROPERTIES = ['name', 'refSchema'];

    private ?string $name = null;

    private ?string $refSchema = null;

    private ?string $refTable = null;

    private ?string $refColumn = null;

    public static function create(array $requiredProperties = []): self
    {
        return new self($requiredProperties);
    }

    public function __construct(array $requiredProperties)
    {
        $this->setRequiredProperties(
            $requiredProperties,
            self::ALWAYS_REQUIRED_PROPERTIES,
            self::OPTIONAL_REQUIRED_PROPERTIES
        );
    }

    public function build(): ForeignKey
    {
        $this->checkRequiredProperties();
        return new ForeignKey(
            $this->name,
            $this->refSchema,
            $this->refTable,
            $this->refColumn
        );
    }

    public function setName(string $name): self
    {
        $name = trim($name);

        if (empty($name)) {
            throw new InvalidArgumentException('ForeignKey\'s name cannot be empty string.');
        }

        $this->name = $name;
        return $this;
    }

    public function setRefSchema(string $refSchema): self
    {
        $refSchema = trim($refSchema);

        if (empty($refSchema)) {
            throw new InvalidArgumentException('ForeignKey\'s refSchema cannot be empty string.');
        }

        $this->refSchema = $refSchema;
        return $this;
    }

    public function setRefTable(string $refTable): self
    {
        $refTable = trim($refTable);

        if (empty($refTable)) {
            throw new InvalidArgumentException('ForeignKey\'s refTable cannot be empty string.');
        }

        $this->refTable = $refTable;
        return $this;
    }

    public function setRefColumn(string $refColumn): self
    {
        $refColumn = trim($refColumn);

        if (empty($refColumn)) {
            throw new InvalidArgumentException('ForeignKey\'s refColumn cannot be empty string.');
        }

        $this->refColumn = $refColumn;
        return $this;
    }
}
