<?php

declare(strict_types=1);

namespace Keboola\DbExtractorSSHTunnel\Test;

use Keboola\DbExtractorSSHTunnel\Exception\UserException;
use Keboola\DbExtractorSSHTunnel\SSHTunnel;
use Monolog\Logger;
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
                'keys' => ['private' => $this->getPrivateKey()],
            ],
            'host' => 'mysql',
            'port' => '3306',
        ];

        $logger = new Logger('test');
        $tunnel = new SSHTunnel($logger);
        $newDbConfig = $tunnel->createSshTunnel($dbConfig);

        $this->assertEquals(
            array_merge(
                $dbConfig,
                ['host' => '127.0.0.1', 'port' => '33306'],
            ),
            $newDbConfig,
        );
    }

    public function testMissingMainParameter(): void
    {
        $dbConfig = [
            'host' => 'testHost',
            'port' => 'testPort',
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
                'keys' => 'anyKey',
            ],
            'host' => 'testHost',
            'port' => 'testPort',
        ];

        $logger = new Logger('test');

        $tunnel = new SSHTunnel($logger);

        self::expectException(UserException::class);
        self::expectExceptionMessage("Parameter 'sshHost' is missing");

        $tunnel->createSshTunnel($dbConfig);
    }

    public function testMaxSshTunnelConnectionRetriesConfig(): void
    {
        $maxRetries = 3;
        $dbConfig = [
            'ssh' => [
                'user' => 'root',
                'sshHost' => 'sshproxy',
                'sshPort' => '222', //wrong port
                'localPort' => '33306',
                'keys' => ['private' => $this->getPrivateKey()],
                'maxRetries' => $maxRetries,
            ],
            'host' => 'mysql',
            'port' => '3306',
        ];

        $logger = new Logger('test');
        $tunnel = new SSHTunnel($logger);

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Unable to create ssh tunnel. Output:  ErrorOutput: ssh: connect to host ' .
        "sshproxy port 222: Connection refused\r\nRetries count: " . $maxRetries);

        $tunnel->createSshTunnel($dbConfig);
    }

    public function getPrivateKey(): string
    {
        return (string) file_get_contents('/root/.ssh/id_rsa');
    }

    public function getPublicKey(): string
    {
        return (string) file_get_contents('/root/.ssh/id_rsa.pub');
    }
}
