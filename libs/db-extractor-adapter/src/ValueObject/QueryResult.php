<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\ValueObject;

use Iterator;
use IteratorAggregate;

/**
 * @extends IteratorAggregate<array>
 *
 * This class wraps and unifies the API of the different DB connections, eg. PDO, ODBC, ...
 * This class can be used as an iterator.
 * Each entry - row is an array, eg [col1 => value1, col2 => value2].
 * Key is the name of the column from DB.
 */
interface QueryResult extends IteratorAggregate
{
    /**
     * @return Iterator<array>
     */
    public function getIterator(): Iterator;

    /**
     * @return array<mixed>|null
     */
    public function fetch(): ?array;
    /**
     * @return array<array<mixed>>
     */
    public function fetchAll(): array;

    public function closeCursor(): void;

    /**
     * Returns low-level result resource or object.
     * @return resource|object
     */
    public function getResource();
}
