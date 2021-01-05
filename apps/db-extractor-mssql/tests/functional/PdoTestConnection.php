<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\FunctionalTests;

use PDO;
use PDOException;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;

class PdoTestConnection
{
    public static function getDbConfigArray(): array
    {
        return [
            'host' => (string) getenv('MSSQL_DB_HOST'),
            'port' => (string) getenv('MSSQL_DB_PORT'),
            'user' => (string) getenv('MSSQL_DB_USER'),
            '#password' => (string) getenv('MSSQL_DB_PASSWORD'),
            'database' => (string) getenv('MSSQL_DB_DATABASE'),
        ];
    }

    public static function createDbConfig(): DatabaseConfig
    {
        return DatabaseConfig::fromArray(self::getDbConfigArray());
    }

    public static function createConnection(): PDO
    {
        $dbConfig = self::createDbConfig();

        $host = $dbConfig->getHost();
        $host .= $dbConfig->hasPort() ? ',' . $dbConfig->getPort() : '';
        $options['Server'] = $host;
        $options['Database'] = $dbConfig->getDatabase();
        if ($dbConfig->hasSSLConnection()) {
            $options['Encrypt'] = 'true';
            $options['TrustServerCertificate'] =
                $dbConfig->getSslConnectionConfig()->isVerifyServerCert() ? 'false' : 'true';
        }

        // ms sql doesn't support options
        try {
            return self::createPdoInstance($dbConfig, $options);
        } catch (PDOException $e) {
            if (strpos($e->getMessage(), 'certificate verify failed:subject name does not match host name') &&
                $dbConfig->hasSSLConnection() &&
                $dbConfig->getSslConnectionConfig()->isIgnoreCertificateCn()
            ) {
                $options['TrustServerCertificate'] = 'true';

                return self::createPdoInstance($dbConfig, $options);
            } else {
                throw $e;
            }
        }
    }

    private static function createPdoInstance(DatabaseConfig $dbConfig, array $options): PDO
    {
        $dsn = sprintf('sqlsrv:%s', implode(';', array_map(function ($key, $item) {
            return sprintf('%s=%s', $key, $item);
        }, array_keys($options), $options)));

        return new PDO($dsn, $dbConfig->getUsername(), $dbConfig->getPassword());
    }
}
