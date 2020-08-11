<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\Tests;

use Ihsw\Toxiproxy\Proxy;
use Ihsw\Toxiproxy\StreamDirections;
use Ihsw\Toxiproxy\ToxicTypes;
use Ihsw\Toxiproxy\Toxiproxy;
use Keboola\DbExtractor\Adapter\Exception\DeadConnectionException;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;
use Keboola\DbExtractor\Adapter\ODBC\OdbcConnection;
use Keboola\DbExtractor\Adapter\ValueObject\QueryResult;
use Keboola\CommonExceptions\UserExceptionInterface;
use Keboola\DbExtractor\Adapter\Connection\DbConnection;

abstract class BaseTest extends TestCase
{
    protected const TOXIPROXY_HOST = 'toxiproxy';

    protected TestLogger $logger;

    protected Toxiproxy $toxiproxy;

    protected function setUp(): void
    {
        parent::setUp();
        $this->logger = new TestLogger();
        $this->toxiproxy = new Toxiproxy('http://toxiproxy:8474');
    }

    protected function tearDown(): void
    {
        parent::tearDown();

        // Clear all proxies
        foreach ($this->toxiproxy->getAll() as $proxy) {
            $this->toxiproxy->delete($proxy);
        }
    }

    protected function createProxyToDb(): Proxy
    {
        return $this->toxiproxy->create('mariadb_proxy', 'mariadb:3306');
    }

    protected function makeProxyDown(Proxy $proxy): void
    {
        $proxy->create(ToxicTypes::TIMEOUT, StreamDirections::DOWNSTREAM, 1.0, [
            'timeout' => 1,
        ]);
    }
}
