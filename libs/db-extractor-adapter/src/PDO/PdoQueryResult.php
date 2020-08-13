<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\PDO;

use Iterator;
use PDO;
use PDOStatement;
use Keboola\DbExtractor\Adapter\ValueObject\QueryResult;

class PdoQueryResult implements QueryResult
{
    protected PDOStatement $stmt;

    protected bool $closed = false;

    public function __construct(PDOStatement $stmt)
    {
        $this->stmt = $stmt;
    }

    public function __destruct()
    {
        $this->closeCursor();
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
            $this->stmt->closeCursor();
            $this->closed = true;
        }
    }

    public function getResource(): PDOStatement
    {
        return $this->stmt;
    }
}
