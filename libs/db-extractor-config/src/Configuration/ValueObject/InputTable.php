<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Configuration\ValueObject;

use Keboola\DbExtractorConfig\Exception\InvalidArgumentException;

class InputTable implements ValueObject
{
    private string $name;

    private string $schema;

    public static function fromArray(array $data): self
    {
        $table = $data['table'];
        return new self($table['tableName'], $table['schema']);
    }

    public function __construct(string $name, string $schema)
    {
        $this->name = $name;
        $this->schema = $schema;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getSchema(): string
    {
        return $this->schema;
    }
}
