<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\FunctionalTests;

use Keboola\DbExtractor\Configuration\ValueObject\MysqlDatabaseConfig;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Extractor\MySQL;
use Keboola\DbExtractor\Extractor\SslHelper;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\Temp\Temp;
use PDO;
use PDOException;
use Throwable;

class PdoTestConnection
{
    public static function getDbConfigArray(): array
    {
        return [
            'host' => (string) getenv('MYSQL_DB_HOST'),
            'port' => (string) getenv('MYSQL_DB_PORT'),
            'user' => (string) getenv('MYSQL_DB_USER'),
            '#password' => (string) getenv('MYSQL_DB_PASSWORD'),
            'database' => (string) getenv('MYSQL_DB_DATABASE'),
        ];
    }

    public static function createDbConfig(): MysqlDatabaseConfig
    {
        $dbConfig = self::getDbConfigArray();
        return MysqlDatabaseConfig::fromArray($dbConfig);
    }

    public static function createConnection(): PDO
    {
        $databaseConfig = self::createDbConfig();

        $options = [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION, // convert errors to PDOExceptions
        ];

        $port = $databaseConfig->hasPort() ? $databaseConfig->getPort() : '3306';

        $dsn = sprintf(
            'mysql:host=%s;port=%s;charset=utf8',
            $databaseConfig->getHost(),
            $port
        );

        if ($databaseConfig->hasDatabase()) {
            $dsn = sprintf(
                'mysql:host=%s;port=%s;dbname=%s;charset=utf8',
                $databaseConfig->getHost(),
                $port,
                $databaseConfig->getDatabase()
            );
        }

        try {
            $pdo = new PDO($dsn, $databaseConfig->getUsername(), $databaseConfig->getPassword(), $options);
        } catch (PDOException $e) {
            throw $e;
        }

        $pdo->setAttribute(PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
        try {
            $pdo->exec('SET NAMES utf8mb4;');
        } catch (PDOException $exception) {
            $pdo->exec('SET NAMES utf8;');
        }

        return $pdo;
    }
}
