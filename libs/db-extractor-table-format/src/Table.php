<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TableResultFormat;

use Keboola\DbExtractor\TableResultFormat\Exception\UserException;

class Table
{
    /** @var string  */
    private $name;

    /** @var string  */
    private $schema;

    /** @var string|null  */
    private $catalog;

    /** @var string|null  */
    private $type;

    /** @var int|null  */
    private $rowCount;

    /** @var TableColumn[] $columns */
    private $columns = [];

    /** @var array  */
    private $requiredParams = ['name', 'schema'];

    /** @var string */
    private $description;

    public function setName(string $name): self
    {
        $this->name = $name;
        return $this;
    }

    public function setSchema(string $schema): self
    {
        $this->schema = $schema;
        return $this;
    }

    public function setCatalog(?string $catalog): self
    {
        $this->catalog = $catalog;
        return $this;
    }

    public function setType(?string $type): self
    {
        $this->type = $type;
        return $this;
    }

    public function setRowCount(?int $rowCount): self
    {
        $this->rowCount = $rowCount;
        return $this;
    }

    public function addColumn(TableColumn $column): self
    {
        $columnKey = $column->getOrdinalPosition() ?: $column->getSanitizedName();
        if (isset($this->columns[$columnKey])) {
            $oldColumn = $this->columns[$columnKey];
            if ($column->isAutoIncrement()) {
                $oldColumn->setAutoIncrement(true);
            }
            if ($column->isPrimaryKey()) {
                $oldColumn->setPrimaryKey(true);
            }
            if ($column->isUniqueKey()) {
                $oldColumn->setUniqueKey(true);
            }
            if ($column->getDefault()) {
                $oldColumn->setDefault($column->getDefault());
            }
            if ($column->getForeignKey() instanceof ForeignKey) {
                $oldColumn->setForeignKey($column->getForeignKey());
            }
            $this->columns[$columnKey] = $oldColumn;
        } else {
            $this->columns[$columnKey] = $column;
        }
        return $this;
    }

    public function setColumns(array $columns): self
    {
        array_walk($columns, function ($item): void {
            if (!($item instanceof TableColumn)) {
                throw new UserException(
                    'Column is not instance \Keboola\DbExtractor\TableResultFormat\TableColumn'
                );
            }
            $this->addColumn($item);
        });
        return $this;
    }

    public function setDescription(string $description): self
    {
        $this->description = $description;
        return $this;
    }

    public function getOutput(): array
    {
        $ret = [];
        foreach (get_class_vars(self::class) as $property => $propertyValue) {
            if ($property === 'requiredParams') {
                continue;
            }
            $propertyValue = $this->{$property} ?? null;
            if (in_array($property, $this->requiredParams) && is_null($propertyValue)) {
                throw new UserException(sprintf(
                    'Parameter \'%s\' is required',
                    $property
                ));
            }

            if (is_array($propertyValue)) {
                foreach ($propertyValue as $item) {
                    $outputValue = $this->getOutputValue($item);
                    if (!is_null($outputValue)) {
                        $ret[$property][] = $outputValue;
                    }
                }
            } else {
                $outputValue = $this->getOutputValue($propertyValue);
                if (!is_null($outputValue)) {
                    $ret[$property] = $outputValue;
                }
            }
        }
        return $ret;
    }

    /**
     * @param TableColumn|mixed $propertyValue
     * @throws UserException
     * @return mixed
     */
    private function getOutputValue($propertyValue)
    {
        if (is_object($propertyValue)) {
            return $propertyValue->getOutput();
        }
        return $propertyValue;
    }
}
