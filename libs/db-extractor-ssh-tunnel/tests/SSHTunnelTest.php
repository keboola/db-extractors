<?php

declare(strict_types=1);

namespace Keboola\DbExtractorSSHTunnel\Test;

use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorLogger\Logger;
use Keboola\DbExtractorSSHTunnel\SSHTunnel;
use PHPUnit\Framework\TestCase;

class SSHTunnelTest extends TestCase
{
    public function testConfig(): void
    {
        $dbConfig = [
            'ssh' => [
                'user' => 'root',
                'sshHost' => 'sshproxy',
                'sshPort' => '22',
                'localPort' => '33306',
                'keys' => ['private' => getenv('SSH_KEY_PRIVATE')],
            ],
            'host' => 'mysql',
            'port' => '3306'
        ];

        $logger = new Logger('test');
        $tunnel = new SSHTunnel($logger);
        $newDbConfig = $tunnel->createSshTunnel($dbConfig);

        $this->assertEquals(
            $newDbConfig,
            array_merge(
                $dbConfig,
                ['host' => '127.0.0.1', 'port' => '33306']
            )
        );
    }

    public function testMissingMainParameter(): void
    {
        $dbConfig = [
            'host' => 'testHost',
            'port' => 'testPort'
        ];

        $logger = new Logger('test');
        $tunnel = new SSHTunnel($logger);

        self::expectException(UserException::class);
        self::expectExceptionMessage("Main parameter 'ssh' is missing");

        $tunnel->createSshTunnel($dbConfig);
    }

    public function testMissingParameter(): void
    {
        $dbConfig = [
            'ssh' => [
                'keys' => 'anyKey'
            ],
            'host' => 'testHost',
            'port' => 'testPort'
        ];

        $logger = new Logger('test');

        $tunnel = new SSHTunnel($logger);

        self::expectException(UserException::class);
        self::expectExceptionMessage("Parameter 'sshHost' is missing");

        $tunnel->createSshTunnel($dbConfig);

    }
}
