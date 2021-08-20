<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\FunctionalTests;

use Keboola\DbExtractor\Configuration\ValueObject\MysqlDatabaseConfig;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Extractor\MySQL;
use Keboola\DbExtractor\Extractor\MySQLDbConnectionFactory;
use Keboola\DbExtractor\Extractor\SslHelper;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\Temp\Temp;
use PDO;
use PDOException;
use Psr\Log\NullLogger;
use Throwable;

class PdoTestConnection
{
    public static function getDbConfigArray(bool $ssl = false): array
    {
        $config = [
            'host' => $ssl ? (string) getenv('MYSQL_DB_SSL_HOST') : (string) getenv('MYSQL_DB_HOST'),
            'port' => (string) getenv('MYSQL_DB_PORT'),
            'user' => (string) getenv('MYSQL_DB_USER'),
            '#password' => (string) getenv('MYSQL_DB_PASSWORD'),
            'database' => (string) getenv('MYSQL_DB_DATABASE'),
            'networkCompression' => false,
        ];

        if ($ssl) {
            $config['ssl'] = [
                'enabled' => true,
                'ca' => (string) getenv('SSL_CA'),
                'cert' => (string) getenv('SSL_CERT'),
                '#key' => (string) getenv('SSL_KEY'),
            ];
        }

        return $config;
    }

    public static function createDbConfig(bool $ssl = false): MysqlDatabaseConfig
    {
        $dbConfig = self::getDbConfigArray($ssl);
        return MysqlDatabaseConfig::fromArray($dbConfig);
    }

    public static function createConnection(bool $ssl = false): PDO
    {
        $dbConfig = self::createDbConfig($ssl);
        return MySQLDbConnectionFactory::create($dbConfig, new NullLogger(), 1)->getConnection();
    }
}
