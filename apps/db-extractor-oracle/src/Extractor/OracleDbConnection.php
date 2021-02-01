<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Adapter\Connection\DbConnection;
use Keboola\DbExtractor\Adapter\ValueObject\QueryResult;
use Keboola\DbExtractor\Exception\ApplicationException;

class OracleDbConnection implements DbConnection
{

    /**
     * @inheritDoc
     */
    public function getConnection()
    {
        throw new ApplicationException('not implemented');
    }

    public function testConnection(): void
    {
        throw new ApplicationException('not implemented');
    }

    public function isAlive(): void
    {
        throw new ApplicationException('not implemented');
    }

    public function quote(string $str): string
    {
        return sprintf('\'%s\'', $str);
    }

    public function quoteIdentifier(string $str): string
    {
        return sprintf('"%s"', $str);
    }

    public function query(string $query, int $maxRetries): QueryResult
    {
        throw new ApplicationException('not implemented');
    }

    public function queryAndProcess(string $query, int $maxRetries, callable $processor): void
    {
        throw new ApplicationException('not implemented');
    }
}
