<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\Tests;

use Ihsw\Toxiproxy\Toxiproxy;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;
use Keboola\DbExtractor\Adapter\Tests\Traits\ToxiProxyTrait;
use Keboola\DbExtractor\Adapter\Tests\Traits\TestDataTrait;

abstract class BaseTest extends TestCase
{
    use TestDataTrait;
    use ToxiProxyTrait;

    protected const TOXIPROXY_HOST = 'toxiproxy';

    protected TestLogger $logger;

    protected function setUp(): void
    {
        parent::setUp();
        $this->logger = new TestLogger();
        $this->connection = $this->createTestConnection();
        $this->toxiproxy = new Toxiproxy('http://toxiproxy:8474');
    }

    protected function tearDown(): void
    {
        parent::tearDown();

        // Clear all proxies
        $this->clearAllProxies();

        // Reset DB
        $this->dropAllTables();
    }
}
