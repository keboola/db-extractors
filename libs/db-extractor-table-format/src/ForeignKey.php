<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TableResultFormat;

use Keboola\DbExtractor\TableResultFormat\Exception\UserException;

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
}
