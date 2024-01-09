<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\Tests;

use Ihsw\Toxiproxy\Toxiproxy;
use Keboola\DbExtractor\Adapter\Tests\Traits\TestDataTrait;
use Keboola\DbExtractor\Adapter\Tests\Traits\ToxiProxyTrait;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

abstract class BaseTest extends TestCase
{
    use TestDataTrait;
    use ToxiProxyTrait;

    protected const TOXIPROXY_HOST = 'toxiproxy';

    protected Temp $temp;

    protected TestLogger $logger;

    protected function setUp(): void
    {
        parent::setUp();
        $this->temp = new Temp(self::class);
        $this->logger = new TestLogger();
        $this->connection = $this->createTestConnection();
        $this->toxiproxy = new Toxiproxy('http://toxiproxy:8474');
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->temp->remove();

        // Clear all proxies
        $this->clearAllProxies();

        // Reset DB
        $this->dropAllTables();
    }

    protected function getCsvFilePath(): string
    {
        return $this->temp->getTmpFolder() . '/out/tables/output.csv';
    }

    protected function createExportConfig(array $data): ExportConfig
    {
        $data['id'] = 123;
        $data['name'] = 'name';
        $data['outputTable'] = 'output';
        $data['retries'] = $data['retries'] ?? 3;
        $data['primaryKey'] = [];
        $data['query'] = $data['query'] ?? null;
        $data['columns'] = $data['columns'] ?? [];
        return ExportConfig::fromArray($data);
    }
}
