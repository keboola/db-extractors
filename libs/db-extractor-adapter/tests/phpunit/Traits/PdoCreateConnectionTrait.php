<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\Tests\Traits;

use Keboola\DbExtractor\Adapter\PDO\PdoConnection;
use Psr\Log\Test\TestLogger;

trait PdoCreateConnectionTrait
{
    protected TestLogger $logger;

    protected function createPdoConnection(?string $host = null, ?int $port = null): PdoConnection
    {
        $dns = sprintf(
            'mysql:host=%s;port=%s;dbname=%s;charset=utf8',
            $host ?? getenv('DB_HOST'),
            $port ?? getenv('DB_PORT'),
            getenv('DB_DATABASE'),
        );
        return new PdoConnection(
            $this->logger,
            $dns,
            (string) getenv('DB_USER'),
            (string) getenv('DB_PASSWORD'),
            [],
        );
    }
}
