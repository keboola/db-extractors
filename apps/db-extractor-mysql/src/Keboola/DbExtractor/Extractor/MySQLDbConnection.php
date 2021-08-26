<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Adapter\PDO\PdoConnection;
use Keboola\DbExtractor\Exception\UserException;
use PDOException;
use Throwable;

class MySQLDbConnection extends PdoConnection
{
    public const CONNECT_MAX_RETRIES = 5;

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
}
