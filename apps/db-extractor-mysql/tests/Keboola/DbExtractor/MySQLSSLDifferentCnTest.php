<?php

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Exception\UserException;

class MySQLSSLDifferentCnTest extends AbstractMySQLTest
{
    public function testCredentials()
    {
        $this->setExpectedException(
            UserException::class,
            'Peer certificate CN=`mysql\' did not match expected CN=`mysql-different-cn'
        );

        $config = $this->getConfig('mysql');
        $config['action'] = 'testConnection';

        $config['parameters']['db']['ssl'] = [
            'enabled' => true,
            'ca' => file_get_contents($this->dataDir . '/mysql/ssl/ca.pem'),
            'cert' => file_get_contents($this->dataDir . '/mysql/ssl/client-cert.pem'),
            'key' => file_get_contents($this->dataDir . '/mysql/ssl/client-key.pem'),
        ];

        unset($config['parameters']['tables']);

        $config['parameters']['db']['host'] = 'mysql-different-cn';

        $this->createApplication($config)->run();
    }
}
