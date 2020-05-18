<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Configuration\ValueObject;

use Keboola\DbExtractorConfig\Exception\InvalidArgumentException;
use Keboola\DbExtractorConfig\Exception\PropertyNotSetException;

class IncrementalFetchingConfig implements ValueObject
{
    private string $column;

    private ?int $limit;

    public static function fromArray(array $data): ?self
    {
        // Enabled ?
        if (empty($data['incremental'])) {
            return null;
        }

        $column = $data['incrementalFetchingColumn'];
        $limit = $data['incrementalFetchingLimit'] ?? null;
        return new self($column, $limit);
    }

    public function __construct(string $column, ?int $limit)
    {
        $this->column = $column;
        $this->limit = $limit;
    }

    public function getColumn(): string
    {
        return $this->column;
    }

    public function hasLimit(): bool
    {
        return $this->limit !== null;
    }

    public function getLimit(): int
    {
        if ($this->limit === null) {
            throw new PropertyNotSetException('Property "limit" is not set.');
        }

        return $this->limit;
    }
}
