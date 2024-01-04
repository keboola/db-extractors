<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\FunctionalTests;

use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use PDO;

class PdoTestConnection
{
    public static function getDbConfigArray(): array
    {
        return [
            'host' => (string) getenv('REDSHIFT_DB_HOST'),
            'port' => (string) getenv('REDSHIFT_DB_PORT'),
            'user' => (string) getenv('REDSHIFT_DB_USER'),
            '#password' => (string) getenv('REDSHIFT_DB_PASSWORD'),
            'database' => (string) getenv('REDSHIFT_DB_DATABASE'),
        ];
    }

    public static function createDbConfig(): DatabaseConfig
    {
        $dbConfig = self::getDbConfigArray();
        return DatabaseConfig::fromArray($dbConfig);
    }

    public static function createConnection(): PDO
    {
        $databaseConfig = self::createDbConfig();

        $dsn = sprintf(
            'pgsql:dbname=%s;port=%s;host=%s',
            $databaseConfig->getDatabase(),
            $databaseConfig->getPort(),
            $databaseConfig->getHost(),
        );
        $pdo = new PDO(
            $dsn,
            $databaseConfig->getUsername(),
            $databaseConfig->getPassword(),
        );
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        return $pdo;
    }
}
