<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Configuration\ValueObject\Serializer;

use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;

class DatabaseConfigSerializer implements IDatabaseConfigSerializer
{
    public static function serialize(DatabaseConfig $databaseConfig): array
    {
        $config = [
            'host' => $databaseConfig->getHost(),
            'user' => $databaseConfig->getUsername(),
            '#password' => $databaseConfig->getPassword(),
        ];
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
