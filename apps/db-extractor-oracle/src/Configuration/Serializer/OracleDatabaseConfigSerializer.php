<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration\Serializer;

use Keboola\DbExtractor\Configuration\OracleDatabaseConfig;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorConfig\Configuration\ValueObject\Serializer\SSLConnectionConfigSerializer;
use Psr\Log\LoggerInterface;

class OracleDatabaseConfigSerializer
{
    public static function serialize(LoggerInterface $logger, OracleDatabaseConfig $databaseConfig): array
    {
        $config = [
            'user' => $databaseConfig->getUsername(),
            '#password' => $databaseConfig->getPassword(),
        ];

        // Connect through
        if ($databaseConfig->isConnectThroughEnabled()) {
            $realUser = (string) getenv('KBC_REALUSER');
            if ($realUser) {
                $logger->info(sprintf(
                    'Connect through is enabled, OracleConnection.PROXY_USER_NAME = "%s".',
                    $realUser,
                ));
                $config['proxyUser'] = $realUser;
            } else {
                throw new UserException(
                    'Connect through is enabled, but "KBC_REALUSER" environment variable is not set.',
                );
            }
        }

        if ($databaseConfig->hasHost()) {
            $config['host'] = $databaseConfig->getHost();
        }
        if ($databaseConfig->hasPort()) {
            $config['port'] = $databaseConfig->getPort();
        }
        if ($databaseConfig->hasDatabase()) {
            $config['database'] = $databaseConfig->getDatabase();
        }
        if ($databaseConfig->hasSchema()) {
            $config['schema'] = $databaseConfig->getSchema();
        }
        if ($databaseConfig->hasSSLConnection()) {
            $config['ssl'] = SSLConnectionConfigSerializer::serialize($databaseConfig->getSslConnectionConfig());
        }

        $config['initQueries'] = $databaseConfig->getInitQueries();

        // Java tool expects string value
        $config['defaultRowPrefetch'] = (string) $databaseConfig->getDefaultRowPrefetch();

        return $config;
    }
}
