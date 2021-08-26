<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\Connection;

use Keboola\DbExtractor\Adapter\Exception\DeadConnectionException;
use Keboola\DbExtractor\Adapter\Exception\UserException;
use Keboola\DbExtractor\Adapter\Exception\UserRetriedException;
use Keboola\DbExtractor\Adapter\ValueObject\QueryResult;
use Psr\Log\LoggerInterface;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;
use Throwable;

abstract class BaseDbConnection implements DbConnection
{
    public const BASE_RETRIED_EXCEPTIONS = [
        DeadConnectionException::class, // see BaseDbConnection:isAlive()];
    ];

    protected LoggerInterface $logger;

    protected int $connectMaxRetries;

    /**
     * Returns low-level connection resource or object.
     * @return resource|object
     */
    abstract public function getConnection();

    abstract public function testConnection(): void;

    abstract public function quote(string $str): string;

    abstract public function quoteIdentifier(string $str): string;

    abstract protected function connect(): void;

    abstract protected function doQuery(string $query): QueryResult;

    abstract protected function getExpectedExceptionClasses(): array;

    public function __construct(LoggerInterface $logger, int $connectMaxRetries = self::CONNECT_DEFAULT_MAX_RETRIES)
    {
        $this->logger = $logger;
        $this->connectMaxRetries = max($connectMaxRetries, 1);
        $this->connectWithRetry();
    }

    public function isAlive(): void
    {
        try {
            $this->testConnection();
        } catch (UserException $e) {
            throw new DeadConnectionException('Dead connection: ' . $e->getMessage());
        }
    }

    public function query(string $query, int $maxRetries = self::DEFAULT_MAX_RETRIES): QueryResult
    {
        return $this->callWithRetry(
            $maxRetries,
            function () use ($query) {
                return $this->queryReconnectOnError($query);
            }
        );
    }

    /**
     * A db error can occur during fetching, so it must be retried together
     * @param callable $processor (QueryResult $dbResult): array
     * @return mixed - returned value from $processor
     */
    public function queryAndProcess(string $query, int $maxRetries, callable $processor)
    {
        return $this->callWithRetry(
            $maxRetries,
            function () use ($query, $processor) {
                $dbResult = $this->queryReconnectOnError($query);
                // A db error can occur during fetching, so it must be wrapped/retried together
                $result = $processor($dbResult);
                // Success of isAlive means that ALL data has been extracted
                $this->isAlive();
                return $result;
            }
        );
    }

    protected function queryReconnectOnError(string $query): QueryResult
    {
        try {
            return $this->doQuery($query);
        } catch (Throwable $e) {
            try {
                // Reconnect
                $this->connect();
            } catch (Throwable $e) {
            };
            throw $e;
        }
    }

    protected function connectWithRetry(): void
    {
        try {
            $this
                ->createRetryProxy($this->connectMaxRetries)
                ->call(function (): void {
                    $this->connect();
                });
        } catch (Throwable $e) {
            throw new UserException('Error connecting to DB: ' . $e->getMessage(), 0, $e);
        }
    }

    /**
     * @return mixed
     */
    protected function callWithRetry(int $maxRetries, callable $callback)
    {
        $proxy = $this->createRetryProxy($maxRetries);
        try {
            return $proxy->call($callback);
        } catch (Throwable $e) {
            throw in_array(get_class($e), $this->getExpectedExceptionClasses(), true) ?
                new UserRetriedException($proxy->getTryCount(), $e->getMessage(), 0, $e) :
                $e;
        }
    }

    protected function createRetryProxy(int $maxRetries): RetryProxy
    {
        $retryPolicy = new SimpleRetryPolicy($maxRetries, $this->getExpectedExceptionClasses());
        $backoffPolicy = new ExponentialBackOffPolicy(1000);
        return new RetryProxy($retryPolicy, $backoffPolicy, $this->logger);
    }
}
