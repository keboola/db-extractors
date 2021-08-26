<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\Tests\Traits;

use Keboola\DbExtractor\Adapter\ODBC\OdbcConnection;
use Psr\Log\Test\TestLogger;

trait OdbcCreateConnectionTrait
{
    protected TestLogger $logger;

    protected function createOdbcConnection(
        ?string $host = null,
        ?int $port = null,
        int $connectRetries = OdbcConnection::CONNECT_DEFAULT_MAX_RETRIES
    ): OdbcConnection {
        $dns = sprintf(
            'Driver={MariaDB ODBC Driver};SERVER=%s;PORT=%d;DATABASE=%s;',
            $host ?? getenv('DB_HOST'),
            $port ?? getenv('DB_PORT'),
            getenv('DB_DATABASE'),
        );
        return new OdbcConnection(
            $this->logger,
            $dns,
            (string) getenv('DB_USER'),
            (string) getenv('DB_PASSWORD'),
            null,
            $connectRetries
        );
    }
}
