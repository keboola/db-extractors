<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\Connection;

use Keboola\DbExtractor\Adapter\ValueObject\QueryResult;

interface DbConnection
{
    public const CONNECT_MAX_RETRIES = 3;

    public const DEFAULT_MAX_RETRIES = 5;

    /**
     * Returns low-level connection resource or object.
     * @return resource|object
     */
    public function getConnection();

    public function testConnection(): void;

    public function isAlive(): void;

    public function quote(string $str): string;

    public function quoteIdentifier(string $str): string;

    public function query(string $query, int $maxRetries): QueryResult;

    /**
     * A db error can occur during fetching, so it must be retried together
     * @param callable $processor (QueryResult $dbResult): array
     * @return mixed - returned value from $processor
     */
    public function queryAndProcess(string $query, int $maxRetries, callable $processor);
}
