<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Configuration\ValueObject\MysqlDatabaseConfig;
use Keboola\Temp\Temp;
use PDO;
use PDOException;
use Psr\Log\LoggerInterface;

class MySQLDbConnectionFactory
{
    // Some SSL keys who worked in Debian Stretch (OpenSSL 1.1.0) stopped working in Debian Buster (OpenSSL 1.1.1).
    // Eg. "Signature Algorithm: sha1WithRSAEncryption" used in mysql5 tests in this repo.
    // This is because Debian wants to be "more secure"
    // and has set "SECLEVEL", which in OpenSSL defaults to "1", to value "2".
    // See https://wiki.debian.org/ContinuousIntegration/TriagingTips/openssl-1.1.1
    // So we reset this value to OpenSSL default.
    public const SSL_DEFAULT_CIPHER_CONFIG = 'DEFAULT@SECLEVEL=1';

    public static function create(
        MysqlDatabaseConfig $dbConfig,
        LoggerInterface $logger,
        int $connectMaxRetries,
    ): MySQLDbConnection {
        // Default options
        $options = [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION, // convert errors to PDOExceptions
            PDO::MYSQL_ATTR_COMPRESS => $dbConfig->isNetworkCompressionEnabled(), // network compression
        ];

        // Ssl encryption
        if ($dbConfig->hasSSLConnection()) {
            $sslConnection = $dbConfig->getSslConnectionConfig();

            $temp = new Temp('mysql-ssl');

            if ($sslConnection->hasKey()) {
                $options[PDO::MYSQL_ATTR_SSL_KEY] = SslHelper::createSSLFile($temp, $sslConnection->getKey());
            }
            if ($sslConnection->hasCert()) {
                $options[PDO::MYSQL_ATTR_SSL_CERT] = SslHelper::createSSLFile($temp, $sslConnection->getCert());
            }
            if ($sslConnection->hasCa()) {
                $options[PDO::MYSQL_ATTR_SSL_CA] = SslHelper::createSSLFile($temp, $sslConnection->getCa());
            }
            if ($sslConnection->hasCipher()) {
                $options[PDO::MYSQL_ATTR_SSL_CIPHER] = $sslConnection->getCipher();
            } else {
                $options[PDO::MYSQL_ATTR_SSL_CIPHER] = self::SSL_DEFAULT_CIPHER_CONFIG;
            }

            if (!$sslConnection->isVerifyServerCert()) {
                $options[PDO::MYSQL_ATTR_SSL_VERIFY_SERVER_CERT] = false;
            }
        }

        // Connection string
        $dsn = [
            'host' => $dbConfig->getHost(),
            'port' => $dbConfig->hasPort() ? $dbConfig->getPort() : '3306',
            'charset' => 'utf8',
        ];

        if ($dbConfig->hasDatabase()) {
            $dsn['dbname'] = $dbConfig->getDatabase();
        }

        // Create connection
        return new MySQLDbConnection(
            $logger,
            self::dsnToString($dsn),
            $dbConfig->getUsername(),
            $dbConfig->getPassword(),
            $options,
            function (PDO $pdo) use ($logger, $dbConfig): void {
                $pdo->setAttribute(PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);

                // Set encoding
                try {
                    $pdo->exec('SET NAMES utf8mb4;');
                } catch (PDOException $exception) {
                    $logger->info('Falling back to "utf8" charset');
                    $pdo->exec('SET NAMES utf8;');
                }

                // Set isolation level
                if ($dbConfig->hasTransactionIsolationLevel()) {
                    $pdo->query(sprintf(
                        'SET SESSION TRANSACTION ISOLATION LEVEL %s',
                        $dbConfig->getTransactionIsolationLevel(),
                    ));
                }
            },
            $connectMaxRetries,
            $dbConfig->getInitQueries(),
        );
    }

    private static function dsnToString(array $values): string
    {
        $pairs = [];
        foreach ($values as $k => $v) {
            $pairs[] = $k . '=' . $v;
        }
        return 'mysql:' . implode(';', $pairs);
    }
}
