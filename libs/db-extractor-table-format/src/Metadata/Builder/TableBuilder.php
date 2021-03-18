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
    public const OPTIONAL_REQUIRED_PROPERTIES = ['schema', 'catalog', 'tablespaceName', 'owner', 'type', 'rowCount'];

    /** @var string[] */
    protected array $columnRequiredProperties;

    private ?string $name = null;

    private ?string $sanitizedName = null;

    private ?string $description = null;

    private ?string $schema = null;

    private ?string $catalog = null;

    private ?string $tablespaceName = null;

    private ?string $owner = null;

    private ?string $type = null;

    private ?int $rowCount = null;

    /**
     * Table can be build without columns if buildColumns = false, see method "setColumnsNotExpected".
     * It is useful when there are a lot of tables and in first step are loaded only tables without columns.
     * Than, in second step, is loaded selected table with columns.
     * @var bool
     */
    private bool $buildColumns = true;

    /** @var ColumnBuilder[] */
    private array $columns = [];

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
            $this->tablespaceName,
            $this->owner,
            $this->type,
            $this->rowCount,
            $this->buildColumns ?
                new ColumnCollection(array_map(fn(ColumnBuilder $col) => $col->build(), $this->columns)) :
                null
        );
    }

    public function setColumnsNotExpected(): self
    {
        $this->setPropertyAsOptional('columns');
        $this->columns = [];
        $this->buildColumns = false;
        return $this;
    }

    public function addColumn(?ColumnBuilder $builder = null): ColumnBuilder
    {
        if ($this->buildColumns === false) {
            throw new InvalidStateException('Columns are not expected.');
        }

        $builder = $builder ?? ColumnBuilder::create($this->columnRequiredProperties);
        $this->columns[] = $builder;
        return $builder;
    }

    public function setName(string $name, bool $trim = true): self
    {
        // Trim can be disabled, eg. in MsSQL is one space valid column name
        $name = $trim ? trim($name) : $name;

        if ($name === '') {
            throw new InvalidArgumentException('Table\'s name cannot be empty.');
        }

        $this->name = $name;
        $this->sanitizedName = BuilderHelper::sanitizeName($name);
        return $this;
    }

    public function setDescription(?string $description): self
    {
        // Trim
        $description = $description !== null ? trim($description) : null;

        // Normalize, empty string is not allowed
        $this->description = $description === '' ? null : $description;
        return $this;
    }

    public function setSchema(?string $schema): self
    {
        // Trim
        $schema = $schema !== null ? trim($schema) : null;

        // Normalize, empty string is not allowed
        $this->schema = $schema === '' ? null : $schema;
        return $this;
    }

    public function setCatalog(?string $catalog): self
    {
        // Trim
        $catalog = $catalog !== null ? trim($catalog) : null;

        // Normalize, empty string is not allowed
        $this->catalog = $catalog === '' ? null : $catalog;
        return $this;
    }

    public function setTablespaceName(?string $tablespaceName): self
    {
        // Trim
        $tablespaceName = $tablespaceName !== null ? trim($tablespaceName) : null;

        // Normalize, empty string is not allowed
        $this->tablespaceName = $tablespaceName === '' ? null : $tablespaceName;
        return $this;
    }

    public function setOwner(?string $owner): self
    {
        // Trim
        $owner = $owner !== null ? trim($owner) : null;

        // Normalize, empty string is not allowed
        $this->owner = $owner === '' ? null : $owner;
        return $this;
    }

    public function setType(string $type): self
    {
        if ($type === '') {
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
