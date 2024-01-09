<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Test\ExtractorTest;
use Keboola\DbExtractorConfig\Config;
use Keboola\DbExtractorConfig\Configuration\ConfigDefinition;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use PHPUnit\Framework\Assert;

class CommonExtractorSslTest extends ExtractorTest
{

    public function testDatabaseConfigSslConnection(): void
    {
        $config = new Config($this->getConfig('common', true), new ConfigDefinition());
        $databaseConfig = DatabaseConfig::fromArray($config->getData()['parameters']['db']);

        Assert::assertTrue($databaseConfig->hasSSlConnection());
        Assert::assertNotEmpty($databaseConfig->getSslConnectionConfig()->getKey());
        Assert::assertNotEmpty($databaseConfig->getSslConnectionConfig()->getCa());
        Assert::assertNotEmpty($databaseConfig->getSslConnectionConfig()->getCert());
    }

    public function testConnection(): void
    {
        $config = $this->getConfig('common', true);
        $config['action'] = 'testConnection';
        $this->getApp(
            $config,
        )->execute();

        Assert::assertTrue(true);
    }


    protected function getConfig(string $driver, bool $sslHost = false): array
    {
        $config = parent::getConfig($driver, $sslHost);
        $config['parameters']['db']['ssl'] = [
            'enabled' => true,
            'ca' => file_get_contents('/ssl-cert/ca-cert.pem'),
            'cert' => file_get_contents('/ssl-cert/client-cert.pem'),
            'key' => file_get_contents('/ssl-cert/client-key.pem'),
        ];
        return $config;
    }
}
