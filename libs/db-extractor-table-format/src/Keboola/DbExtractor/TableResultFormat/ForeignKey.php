<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TableResultFormat;

use Keboola\DbExtractor\Exception\UserException;

class ForeignKey
{
    /** @var string  */
    private $name;

    /** @var string  */
    private $refSchema;

    /** @var string  */
    private $refTable;

    /** @var string  */
    private $refColumn;

    /** @var array  */
    private $requiredParams = ['name', 'refSchema', 'refTable', 'refColumn'];

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

    public function getName(): string
    {
        return $this->name;
    }

    public function getRefSchema(): string
    {
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

            if (!is_null($propertyValue)) {
                $ret[$property] = $this->{$property};
            }
        }
        return $ret;
    }
}
