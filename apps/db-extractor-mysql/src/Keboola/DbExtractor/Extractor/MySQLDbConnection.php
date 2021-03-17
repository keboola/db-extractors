<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Exception\UserException;
use PDO;
use Keboola\DbExtractor\Adapter\PDO\PdoConnection;
use PDOException;
use Throwable;

class MySQLDbConnection extends PdoConnection
{
    protected function connect(): void
    {
        $this->logger->info(sprintf('Creating PDO connection to "%s".', $this->dsn));
        try {
            $this->pdo = new PDO($this->dsn, $this->user, $this->password, $this->options);
        } catch (PDOException $e) {
            $this->handleException($e);
        }

        if ($this->init) {
            ($this->init)($this->pdo);
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

        // SQLSTATE[HY000] is general error without message, so throw previous exception
        if (strpos($e->getMessage(), 'SQLSTATE[HY000]') === 0 && $e->getPrevious() !== null) {
            throw $e->getPrevious();
        }
        throw $e;
    }
}
