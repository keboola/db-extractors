<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\ODBC;

use Iterator;
use Keboola\DbExtractor\Adapter\ValueObject\QueryMetadata;
use Keboola\DbExtractor\Adapter\ValueObject\QueryResult;

class OdbcQueryResult implements QueryResult
{
    protected string $query;

    protected QueryMetadata $queryMetadata;

    /** @var resource */
    protected $stmt;

    protected bool $closed = false;

    /**
     * @param resource $stmt
     */
    public function __construct(string $query, QueryMetadata $queryMetadata, $stmt)
    {
        $this->query = $query;
        $this->queryMetadata = $queryMetadata;
        $this->stmt = $stmt;
    }

    public function __destruct()
    {
        $this->closeCursor();
    }

    public function getQuery(): string
    {
        return $this->query;
    }

    public function getMetadata(): QueryMetadata
    {
        return $this->queryMetadata;
    }
    /**
     * @return Iterator<array>
     */
    public function getIterator(): Iterator
    {
        while ($row = $this->fetch()) {
            yield $row;
        }
    }

    /**
     * @return array<mixed>|null
     */
    public function fetch(): ?array
    {
        /** @var array|false $result */
        $result = odbc_fetch_array($this->stmt);
        return $result === false ? null : $result;
    }

    /**
     * @return array<array<mixed>>
     */
    public function fetchAll(): array
    {
        return iterator_to_array($this->getIterator());
    }

    public function closeCursor(): void
    {
        if ($this->closed === false) {
            odbc_free_result($this->stmt);
            $this->closed = true;
        }
    }

    /**
     * @return resource
     */
    public function getResource()
    {
        return $this->stmt;
    }
}
