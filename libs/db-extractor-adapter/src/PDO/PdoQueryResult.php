<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\PDO;

use Iterator;
use Keboola\DbExtractor\Adapter\ValueObject\QueryMetadata;
use PDO;
use PDOException;
use PDOStatement;
use Keboola\DbExtractor\Adapter\ValueObject\QueryResult;

class PdoQueryResult implements QueryResult
{
    protected string $query;

    protected QueryMetadata $queryMetadata;

    protected PDOStatement $stmt;

    protected bool $closed = false;

    public function __construct(string $query, QueryMetadata $queryMetadata, PDOStatement $stmt)
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
        while ($row = $this->stmt->fetch(PDO::FETCH_ASSOC)) {
            yield $row;
        }
    }

    /**
     * @return array<mixed>|null
     */
    public function fetch(): ?array
    {
        $result = $this->stmt->fetch(PDO::FETCH_ASSOC);
        return $result === false ? null : $result;
    }

    /**
     * @return array<array<mixed>>
     */
    public function fetchAll(): array
    {
        /** @var array $result - errrors are converted to exceptions */
        $result = $this->stmt->fetchAll(PDO::FETCH_ASSOC);
        return $result;
    }

    public function closeCursor(): void
    {
        if ($this->closed === false) {
            try {
                $this->stmt->closeCursor();
            } catch (PDOException $e) {}
            $this->closed = true;
        }
    }

    public function getResource(): PDOStatement
    {
        return $this->stmt;
    }
}
