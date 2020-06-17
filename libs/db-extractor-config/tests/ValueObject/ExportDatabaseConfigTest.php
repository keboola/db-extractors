<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Tests\ValueObject;

use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportDatabaseConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\SSLConnectionConfig;
use Keboola\DbExtractorConfig\Exception\PropertyNotSetException;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

class ExportDatabaseConfigTest extends TestCase
{
    public function testExportConfig(): void
    {
        $config = [
            'host' => 'testHost.local',
            'port' => 12345,
            'username' => 'username',
            '#password' => 'secretPassword',
            'database' => 'database',
            'schema' => 'schema',
            'ssl' => [
                'key' => 'testKey',
                'ca' => 'testCa',
                'cert' => 'testCert',
            ],
        ];

        $exportDatabaseConfig = ExportDatabaseConfig::fromArray($config);

        Assert::assertTrue($exportDatabaseConfig->hasPort());
        Assert::assertTrue($exportDatabaseConfig->hasDatabase());
        Assert::assertTrue($exportDatabaseConfig->hasSchema());
        Assert::assertTrue($exportDatabaseConfig->hasSSLConnection());

        Assert::assertEquals('testHost.local', $exportDatabaseConfig->getHost());
        Assert::assertEquals(12345, $exportDatabaseConfig->getPort());
        Assert::assertEquals('secretPassword', $exportDatabaseConfig->getPassword());
        Assert::assertEquals('database', $exportDatabaseConfig->getDatabase());
        Assert::assertEquals('schema', $exportDatabaseConfig->getSchema());

        Assert::assertInstanceOf(SSLConnectionConfig::class, $exportDatabaseConfig->getSslConnectionConfig());
    }

    public function testOnlyRequiredProperties(): void
    {
        $config = [
            'host' => 'testHost.local',
            'username' => 'username',
            '#password' => 'secretPassword',
        ];

        $exportDatabaseConfig = ExportDatabaseConfig::fromArray($config);

        Assert::assertFalse($exportDatabaseConfig->hasPort());
        Assert::assertFalse($exportDatabaseConfig->hasDatabase());
        Assert::assertFalse($exportDatabaseConfig->hasSchema());
        Assert::assertFalse($exportDatabaseConfig->hasSSLConnection());

        try {
            $exportDatabaseConfig->getPort();
            Assert::fail('Property "port" is exists.');
        } catch (PropertyNotSetException $e) {
            Assert::assertEquals('Property "port" is not set.', $e->getMessage());
        }

        try {
            $exportDatabaseConfig->getDatabase();
            Assert::fail('Property "database" is exists.');
        } catch (PropertyNotSetException $e) {
            Assert::assertEquals('Property "database" is not set.', $e->getMessage());
        }

        try {
            $exportDatabaseConfig->getSslConnectionConfig();
            Assert::fail('SSL config is set.');
        } catch (PropertyNotSetException $e) {
            Assert::assertEquals('SSL config is not set.', $e->getMessage());
        }

        try {
            $exportDatabaseConfig->getPort();
            Assert::fail('Property "port" is exists.');
        } catch (PropertyNotSetException $e) {
            Assert::assertEquals('Property "port" is not set.', $e->getMessage());
        }
    }

    /**
     * @dataProvider exportMissingRequiredPropertiesProvider
     */
    public function testExportMissingRequiredProperties(array $config, string $expectedExceptionMessage): void
    {
        try {
            ExportDatabaseConfig::fromArray($config);
        } catch (PropertyNotSetException $e) {
            Assert::assertEquals($expectedExceptionMessage, $e->getMessage());
        }
    }

    public function exportMissingRequiredPropertiesProvider(): array
    {
        return [
            [
                [
                    'username' => 'username',
                    '#password' => 'secretPassword',
                ],
                'Property "host" does not exists',
            ],
            [
                [
                    'host' => 'testHost.local',
                    '#password' => 'secretPassword',
                ],
                'Property "username" does not exists',
            ],
            [
                [
                    'host' => 'testHost.local',
                    'username' => 'username',
                ],
                'Property "#password" does not exists',
            ],
        ];
    }
}
