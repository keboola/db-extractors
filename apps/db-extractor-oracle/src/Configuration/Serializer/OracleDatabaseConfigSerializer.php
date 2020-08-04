<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration\Serializer;

use Keboola\DbExtractor\Configuration\OracleDatabaseConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\Serializer\SSLConnectionConfigSerializer;

class OracleDatabaseConfigSerializer
{
    public static function serialize(OracleDatabaseConfig $databaseConfig): array
    {
        $config = [
            'user' => $databaseConfig->getUsername(),
            '#password' => $databaseConfig->getPassword(),
        ];
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
        return $config;
    }
}
