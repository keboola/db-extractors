<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TableResultFormat;

use Keboola\DbExtractor\Exception\UserException;

class ForeignKey
{
    private string $name;

    private string $refSchema;

    private string $refTable;

    private string $refColumn;

    private array $requiredParams = ['name', 'refSchema', 'refTable', 'refColumn'];

    public function setName(string $name): self
    {
        $this->name = $name;
        return $this;
    }

    public function setRefSchema(string $refSchema): self
    {
        $this->refSchema = $refSchema;
        return $this;
    }

    public function setRefTable(string $refTable): self
    {
        $this->refTable = $refTable;
        return $this;
    }

    public function setRefColumn(string $refColumn): self
    {
        $this->refColumn = $refColumn;
        return $this;
    }

    public function getOutput(): array
    {
        $ret = [];
        foreach (get_class_vars(self::class) as $property => $propertyValue) {
            if ($property === 'requiredParams') {
                continue;
            }
            if (in_array($property, $this->requiredParams) && is_null($propertyValue)) {
                throw new UserException(sprintf(
                    'Parameter \'%s\' is required',
                    $property
                ));
            }

            if (!is_null($propertyValue)) {
                $ret[$property] = $this->{$property};
            }
        }
        return $ret;
    }
}
