<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TableResultFormat\Metadata\Builder;

use Keboola\DbExtractor\TableResultFormat\Exception\InvalidArgumentException;
use Keboola\DbExtractor\TableResultFormat\Exception\InvalidStateException;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\ColumnCollection;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\Table;
use Keboola\Utils\Sanitizer\ColumnNameSanitizer;

class TableBuilder implements Builder
{
    use RequiredPropertiesTrait;

    /**
     * List of always required properties
     */
    public const ALWAYS_REQUIRED_PROPERTIES = ['name', 'sanitizedName', 'columns'];

    /**
     * List of properties that can be marked as required through the constructor.
     */
    public const OPTIONAL_REQUIRED_PROPERTIES = ['schema', 'catalog', 'type', 'rowCount'];

    /** @var string[] */
    protected array $columnRequiredProperties;

    private ?string $name = null;

    private ?string $sanitizedName = null;

    private ?string $description = null;

    private ?string $schema = null;

    private ?string $catalog = null;

    private ?string $type = null;

    private ?int $rowCount = null;

    /** @var null|ColumnBuilder[] */
    private ?array $columns = [];

    public static function create(array $requiredProperties = [], array $columnRequiredProperties = []): self
    {
        return new self($requiredProperties, $columnRequiredProperties);
    }

    protected function __construct(array $requiredProperties, array $columnRequiredProperties)
    {
        $this->setRequiredProperties(
            $requiredProperties,
            self::ALWAYS_REQUIRED_PROPERTIES,
            self::OPTIONAL_REQUIRED_PROPERTIES
        );
        $this->columnRequiredProperties = $columnRequiredProperties;
    }

    public function build(): Table
    {
        $this->checkRequiredProperties();
        return new Table(
            $this->name,
            $this->sanitizedName,
            $this->description,
            $this->schema,
            $this->catalog,
            $this->type,
            $this->rowCount,
            $this->columns !== null ?
                new ColumnCollection(array_map(fn(ColumnBuilder $col) => $col->build(), $this->columns)) :
                null
        );
    }

    public function setColumnsNotExpected(): self
    {
        $this->setPropertyAsOptional('columns');
        $this->columns = null;
        return $this;
    }

    public function addColumn(?ColumnBuilder $builder = null): ColumnBuilder
    {
        if ($this->columns === null) {
            throw new InvalidStateException('Columns are not expected.');
        }

        $builder = $builder ?? ColumnBuilder::create($this->columnRequiredProperties);
        $this->columns[] = $builder;
        return $builder;
    }

    public function setName(string $name): self
    {
        // Trim
        $name = trim($name);

        if (empty($name)) {
            throw new InvalidArgumentException('Table\'s name cannot be empty.');
        }

        $this->name = $name;
        $this->sanitizedName = ColumnNameSanitizer::sanitize($name);
        return $this;
    }

    public function setDescription(?string $description): self
    {
        // Trim
        $description = $description !== null ? trim($description) : null;

        // Normalize, empty string is not allowed
        $this->description = empty($description) ? null : $description;
        return $this;
    }

    public function setSchema(?string $schema): self
    {
        // Trim
        $schema = $schema !== null ? trim($schema) : null;

        // Normalize, empty string is not allowed
        $this->schema = empty($schema) ? null : $schema;
        return $this;
    }

    public function setCatalog(?string $catalog): self
    {
        // Trim
        $catalog = $catalog !== null ? trim($catalog) : null;

        // Normalize, empty string is not allowed
        $this->catalog = empty($catalog) ? null : $catalog;
        return $this;
    }

    public function setType(string $type): self
    {
        if (empty($type)) {
            throw new InvalidArgumentException('Table\'s type cannot be empty string.');
        }

        $this->type = $type;
        return $this;
    }

    public function setRowCount(?int $rowsCount): self
    {
        $this->rowCount = $rowsCount;
        return $this;
    }
}
