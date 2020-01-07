<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TableResultFormat;

use Keboola\DbExtractor\Exception\UserException;
use Keboola\Utils;

class TableColumn
{
    /** @var string  */
    private $name;

    /** @var string  */
    private $sanitizedName;

    /** @var string  */
    private $type;

    /** @var bool  */
    private $primaryKey = false;

    /** @var bool  */
    private $uniqueKey = false;

    /** @var string|null  */
    private $length;

    /** @var bool  */
    private $nullable;

    /** @var string|null  */
    private $default;

    /** @var int|null  */
    private $ordinalPosition;

    /** @var string|null  */
    private $description;

    /** @var bool  */
    private $autoIncrement;

    /** @var int */
    private $autoIncrementValue;

    /** @var ForeignKey|null  */
    private $foreignKey;

    /** @var array $requiredParams */
    private $requiredParams = ['name', 'type'];

    public function setName(string $name): self
    {
        $this->name = $name;
        $this->sanitizedName = Utils\sanitizeColumnName($name);
        return $this;
    }

    public function getSanitizedName(): string
    {
        return $this->sanitizedName;
    }

    public function setType(string $type): self
    {
        $this->type = $type;
        return $this;
    }

    public function setPrimaryKey(bool $primaryKey): self
    {
        $this->primaryKey = $primaryKey;
        return $this;
    }

    public function setUniqueKey(bool $uniqueKey): self
    {
        $this->uniqueKey = $uniqueKey;
        return $this;
    }

    public function setLength(?string $length): self
    {
        $this->length = $length;
        return $this;
    }

    public function setNullable(bool $nullable): self
    {
        $this->nullable = $nullable;
        return $this;
    }

    public function setDefault(?string $default): self
    {
        $this->default = $default;
        return $this;
    }

    public function setOrdinalPosition(?int $ordinalPosition): self
    {
        $this->ordinalPosition = $ordinalPosition;
        return $this;
    }

    public function getOrdinalPosition(): ?int
    {
        return $this->ordinalPosition;
    }

    public function setDescription(?string $description): self
    {
        $this->description = $description;
        return $this;
    }

    public function setAutoIncrement(bool $autoIncrement): self
    {
        $this->autoIncrement = $autoIncrement;
        return $this;
    }

    public function setAutoIncrementValue(int $autoIncrementValue): self
    {
        $this->autoIncrementValue = $autoIncrementValue;
        return $this;
    }

    public function setForeignKey(?ForeignKey $foreignKey): self
    {
        $this->foreignKey = $foreignKey;
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

            if ($propertyValue instanceof ForeignKey) {
                $ret['foreignKey'] = true;
                $ret['foreignKeyName'] = $propertyValue->getName();
                $ret['foreignKeyRefSchema'] = $propertyValue->getRefSchema();
                $ret['foreignKeyRefTable'] = $propertyValue->getRefTable();
                $ret['foreignKeyRefColumn'] = $propertyValue->getRefColumn();
            } elseif (!is_null($propertyValue)) {
                $ret[$property] = $this->{$property};
            }
        }
        return $ret;
    }
}
