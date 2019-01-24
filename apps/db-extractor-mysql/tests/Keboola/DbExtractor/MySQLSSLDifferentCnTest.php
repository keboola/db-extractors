<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Exception\UserException;

class MySQLSSLDifferentCnTest extends AbstractMySQLTest
{
    public function testCredentials(): void
    {
        $this->setExpectedException(
            UserException::class,
            'Peer certificate CN=`mysql\' did not match expected CN=`mysql-different-cn'
        );

        $config = $this->getConfig();
        $config['action'] = 'testConnection';

        $config['parameters']['db']['ssl'] = [
            'enabled' => true,
            'ca' => file_get_contents($this->dataDir . '/mysql/ssl/ca.pem'),
            'cert' => file_get_contents($this->dataDir . '/mysql/ssl/client-cert.pem'),
            'key' => file_get_contents($this->dataDir . '/mysql/ssl/client-key.pem'),
        ];

        $config['parameters']['tables'] = [];

        $config['parameters']['db']['host'] = 'mysql-different-cn';

        $this->createApplication($config)->run();
    }

    public function testAllowInvalidHostOption(): void
    {
        $config = $this->getConfig();
        $config['action'] = 'testConnection';

        $config['parameters']['db']['ssl'] = [
            'enabled' => true,
            'ca' => file_get_contents($this->dataDir . '/mysql/ssl/ca.pem'),
            'cert' => file_get_contents($this->dataDir . '/mysql/ssl/client-cert.pem'),
            'key' => file_get_contents($this->dataDir . '/mysql/ssl/client-key.pem'),
            'allowInvalidHost' => true,
        ];

        $config['parameters']['tables'] = [];

        $config['parameters']['db']['host'] = 'mysql-different-cn';

        $result = $this->createApplication($config)->run();
        
        $this->assertEquals("success", $result['status']);
    }
}
