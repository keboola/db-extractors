<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Adapter\PDO\PdoConnection;
use Keboola\DbExtractor\Exception\UserException;
use PDOException;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;
use Throwable;

class MySQLDbConnection extends PdoConnection
{
    public const CONNECT_MAX_RETRIES = 5;

    private const RETRY_MAX_INTERVAL = 2 * 60 * 1000; // 2 minutes in miliseconds

    protected function connect(): void
    {
        try {
            parent::connect();
        } catch (PDOException $e) {
            $this->handleException($e);
        }
    }

    public function handleException(Throwable $e): void
    {
        $checkCnMismatch = function (Throwable $exception): void {
            if (strpos($exception->getMessage(), 'did not match expected CN') !== false) {
                throw new UserException($exception->getMessage());
            }
        };
        $checkCnMismatch($e);
        $previous = $e->getPrevious();
        if ($previous !== null) {
            $checkCnMismatch($previous);
        }

        // SQLSTATE[HY000] is a main general message and additional informations are in the previous exception, so throw previous
        if (strpos($e->getMessage(), 'SQLSTATE[HY000]') === 0 && $e->getPrevious() !== null) {
            throw $e->getPrevious();
        }
        throw $e;
    }

    protected function createRetryProxy(int $maxRetries): RetryProxy
    {
        $retryPolicy = new SimpleRetryPolicy($maxRetries, $this->getExpectedExceptionClasses());
        $backoffPolicy = new ExponentialBackOffPolicy(2000, null, self::RETRY_MAX_INTERVAL);
        return new RetryProxy($retryPolicy, $backoffPolicy, $this->logger);
    }
}
