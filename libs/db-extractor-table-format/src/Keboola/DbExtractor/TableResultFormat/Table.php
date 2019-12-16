<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TableResultFormat;

use Keboola\DbExtractor\Exception\UserException;

class Table
{
    private string $name;

    private string $catalog;

    private string $schema;

    private string $type;

    private int $rowCount;

    /** @var TableColumn[] $columns */
    private array $columns = [];

    private array $requiredParams = ['name', 'schema'];

    public function setName(string $name): self
    {
        $this->name = $name;
        return $this;
    }

    public function setCatalog(string $catalog): self
    {
        $this->catalog = $catalog;
        return $this;
    }

    public function setSchema(string $schema): self
    {
        $this->schema = $schema;
        return $this;
    }

    public function setType(string $type): self
    {
        $this->type = $type;
        return $this;
    }

    public function setRowCount(int $rowCount): self
    {
        $this->rowCount = $rowCount;
        return $this;
    }

    public function addColumn(TableColumn $column): self
    {
        $this->columns[] = $column;
        return $this;
    }

    public function setColumns(array $columns): self
    {
        array_walk($columns, function ($item) {
            if (!($item instanceof TableColumn)) {
                throw new UserException(
                    'Column is not instance \Keboola\DbExtractor\TableResultFormat\TableColumn'
                );
            }
        });
        $this->columns = $columns;
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
