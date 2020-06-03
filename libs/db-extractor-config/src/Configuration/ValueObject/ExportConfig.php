<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Configuration\ValueObject;

use Keboola\DbExtractorConfig\Exception\PropertyNotSetException;

class ExportConfig implements ValueObject
{
    /** Id of config row or tables config item */
    private ?string $configId;

    /** Name of config row or tables config item */
    private ?string $configName;

    /** Custom export query */
    private ?string $query;

    /** Table that will be exported (if query is not set) */
    private ?InputTable $table;

    /** Configuration of incremental fetching */
    private ?IncrementalFetchingConfig $incrementalFetchingConfig;

    /** Columns that will be exported, cannot be used with query, if empty => all columns */
    private array $columns;

    /** Name of the output table */
    private string $outputTable;

    /** PK key can be composed of multiple columns, therefore an array */
    private array $primaryKey;

    /** Number of max retries if an error occurs */
    private int $maxRetries;

    public static function fromArray(array $data): self
    {
        return new self(
            $data['id'] ?? null,
            $data['name'] ?? null,
            $data['query'],
            empty($data['query']) ? InputTable::fromArray($data) : null,
            empty($data['query']) ? IncrementalFetchingConfig::fromArray($data) : null,
            $data['columns'],
            $data['outputTable'],
            $data['primaryKey'],
            $data['retries']
        );
    }

    public function __construct(
        ?string $configId,
        ?string $configName,
        ?string $query,
        ?InputTable $table,
        ?IncrementalFetchingConfig $incrementalFetchingConfig,
        array $columns,
        string $outputTable,
        array $primaryKey,
        int $maxRetries
    ) {
        $this->configId = $configId;
        $this->configName = $configName;
        $query = $query !== null ? trim($query) : null;
        $outputTable = trim($outputTable);
        $this->query = $query;
        $this->table = $table;
        $this->incrementalFetchingConfig = $incrementalFetchingConfig;
        $this->columns = $columns;
        $this->outputTable = $outputTable;
        $this->primaryKey = $primaryKey;
        $this->maxRetries = $maxRetries;
    }

    public function hasConfigId(): bool
    {
        return $this->configId !== null;
    }

    public function getConfigId(): string
    {
        if ($this->configId === null) {
            throw new PropertyNotSetException('Config id is not set.');
        }

        return $this->configId;
    }

    public function hasConfigName(): bool
    {
        return $this->configName !== null;
    }

    public function getConfigName(): string
    {
        if ($this->configName === null) {
            throw new PropertyNotSetException('Config name is not set.');
        }

        return $this->configName;
    }

    public function hasTable(): bool
    {
        return $this->table !== null;
    }

    public function getTable(): InputTable
    {
        if ($this->table === null) {
            throw new PropertyNotSetException('Table is not set, use "query".');
        }

        return $this->table;
    }

    public function isIncrementalFetching(): bool
    {
        return $this->incrementalFetchingConfig !== null;
    }

    public function hasIncrementalFetchingLimit(): bool
    {
        if ($this->incrementalFetchingConfig === null) {
            return false;
        }

        return $this->incrementalFetchingConfig->hasLimit();
    }

    public function getIncrementalFetchingLimit(): int
    {
        if ($this->incrementalFetchingConfig === null) {
            throw new PropertyNotSetException('Incremental fetching is not enabled.');
        }

        return $this->incrementalFetchingConfig->getLimit();
    }

    public function getIncrementalFetchingColumn(): string
    {
        if ($this->incrementalFetchingConfig === null) {
            throw new PropertyNotSetException('Incremental fetching is not enabled.');
        }

        return $this->incrementalFetchingConfig->getColumn();
    }

    public function getIncrementalFetchingConfig(): IncrementalFetchingConfig
    {
        if ($this->incrementalFetchingConfig === null) {
            throw new PropertyNotSetException('Incremental fetching is not enabled.');
        }

        return $this->incrementalFetchingConfig;
    }

    public function hasQuery(): bool
    {
        return $this->query !== null;
    }

    public function getQuery(): string
    {
        if ($this->query === null) {
            throw new PropertyNotSetException('Query is not set.');
        }

        return $this->query;
    }

    public function hasColumns(): bool
    {
        return !empty($this->columns);
    }

    public function getColumns(): array
    {
        if (empty($this->columns)) {
            throw new PropertyNotSetException('Columns are not set.');
        }

        return $this->columns;
    }

    public function getOutputTable(): string
    {
        return $this->outputTable;
    }

    public function hasPrimaryKey(): bool
    {
        return !empty($this->primaryKey);
    }

    public function getPrimaryKey(): array
    {
        if (empty($this->primaryKey)) {
            throw new PropertyNotSetException('Primary key is not set.');
        }

        return $this->primaryKey;
    }

    public function getMaxRetries(): int
    {
        return $this->maxRetries;
    }
}
