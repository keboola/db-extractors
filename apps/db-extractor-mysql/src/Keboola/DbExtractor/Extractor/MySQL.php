<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbExtractorConfig\Tests\ValueObject\DatabaseConfigTest;
use PDO;
use PDOException;
use Throwable;
use Keboola\DbExtractor\TableResultFormat\Exception\ColumnNotFoundException;
use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\MySQL as MysqlDatatype;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Keboola\Temp\Temp;

class MySQL extends BaseExtractor
{
    // Some SSL keys who worked in Debian Stretch (OpenSSL 1.1.0) stopped working in Debian Buster (OpenSSL 1.1.1).
    // Eg. "Signature Algorithm: sha1WithRSAEncryption" used in mysql5 tests in this repo.
    // This is because Debian wants to be "more secure"
    // and has set "SECLEVEL", which in OpenSSL defaults to "1", to value "2".
    // See https://wiki.debian.org/ContinuousIntegration/TriagingTips/openssl-1.1.1
    // So we reset this value to OpenSSL default.
    public const SSL_CIPHER_CONFIG = 'DEFAULT@SECLEVEL=1';
    public const INCREMENTAL_TYPES = ['INTEGER', 'NUMERIC', 'FLOAT', 'TIMESTAMP'];

    protected ?string $database = null;

    public function getMetadataProvider(): MetadataProvider
    {
        return new MySQLMetadataProvider($this->db, $this->database);
    }

    public function createConnection(DatabaseConfig $databaseConfig): PDO
    {
        $isSsl = false;
        $isCompression = !empty($params['networkCompression']) ? true :false;

        $options = [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION, // convert errors to PDOExceptions
            PDO::MYSQL_ATTR_COMPRESS => $isCompression, // network compression
        ];

        // ssl encryption
        if ($databaseConfig->hasSSLConnection()) {
            $sslConnection = $databaseConfig->getSslConnectionConfig();

            $temp = new Temp('myslq-ssl');

            if ($sslConnection->hasKey()) {
                $options[PDO::MYSQL_ATTR_SSL_KEY] = SslHelper::createSSLFile($temp, $sslConnection->getKey());
                $isSsl = true;
            }
            if ($sslConnection->getCert()) {
                $options[PDO::MYSQL_ATTR_SSL_CERT] = SslHelper::createSSLFile($temp, $sslConnection->getCert());
                $isSsl = true;
            }
            if ($sslConnection->getCa()) {
                $options[PDO::MYSQL_ATTR_SSL_CA] = SslHelper::createSSLFile($temp, $sslConnection->getCa());
                $isSsl = true;
            }
            if ($sslConnection->hasCipher()) {
                $options[PDO::MYSQL_ATTR_SSL_CIPHER] = $sslConnection->getCipher();
            }
            if (!$sslConnection->isVerifyServerCert()) {
                $options[PDO::MYSQL_ATTR_SSL_VERIFY_SERVER_CERT] = false;
            }

            $options[PDO::MYSQL_ATTR_SSL_CIPHER] = self::SSL_CIPHER_CONFIG;
        }

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
            $this->database = $databaseConfig->getDatabase();
        }

        $this->logger->info("Connecting to DSN '" . $dsn . "' " . ($isSsl ? 'Using SSL' : ''));

        try {
            $pdo = new PDO($dsn, $databaseConfig->getUsername(), $databaseConfig->getPassword(), $options);
        } catch (PDOException $e) {
            $checkCnMismatch = function (Throwable $exception): void {
                if (strpos($exception->getMessage(), 'did not match expected CN') !== false) {
                    throw new UserException($exception->getMessage());
                }
            };
            $checkCnMismatch($e);
            $previous = $e->getPrevious();
            if ($previous !== null) {
                $checkCnMismatch($previous);
            }
            throw $e;
        }
        $pdo->setAttribute(PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
        try {
            $pdo->exec('SET NAMES utf8mb4;');
        } catch (PDOException $exception) {
            $this->logger->info('Falling back to "utf8" charset');
            $pdo->exec('SET NAMES utf8;');
        }

        if ($isSsl) {
            $status = $pdo->query("SHOW STATUS LIKE 'Ssl_cipher';")->fetch(PDO::FETCH_ASSOC);

            if (empty($status['Value'])) {
                throw new UserException(sprintf('Connection is not encrypted'));
            } else {
                $this->logger->info('Using SSL cipher: ' . $status['Value']);
            }
        }

        if ($isCompression) {
            $status = $pdo->query("SHOW SESSION STATUS LIKE 'Compression';")->fetch(PDO::FETCH_ASSOC);

            if (empty($status['Value']) || $status['Value'] !== 'ON') {
                throw new UserException(sprintf('Network communication is not compressed'));
            } else {
                $this->logger->info('Using network communication compression');
            }
        }

        return $pdo;
    }

    public function getConnection(): PDO
    {
        return $this->db;
    }

    public function testConnection(): void
    {
        $this->db->query('SELECT NOW();')->execute();
    }

    public function export(ExportConfig $exportConfig): array
    {
        // if database set make sure the database and selected table schema match
        if ($this->database && $exportConfig->hasTable() &&
            mb_strtolower($this->database) !== mb_strtolower($exportConfig->getTable()->getSchema())
        ) {
            throw new UserException(sprintf(
                'Invalid Configuration [%s].  The table schema "%s" is different from the connection database "%s"',
                $exportConfig->getTable()->getName(),
                $exportConfig->getTable()->getSchema(),
                $this->database
            ));
        }

        return parent::export($exportConfig);
    }

    public function validateIncrementalFetching(ExportConfig $exportConfig): void
    {
        try {
            $column = $this
                ->getMetadataProvider()
                ->getTable($exportConfig->getTable())
                ->getColumns()
                ->getByName($exportConfig->getIncrementalFetchingColumn());
        } catch (ColumnNotFoundException $e) {
            throw new UserException(
                sprintf(
                    'Column "%s" specified for incremental fetching was not found in the table',
                    $exportConfig->getIncrementalFetchingColumn()
                )
            );
        }

        try {
            $datatype = new MysqlDatatype($column->getType());
            $type = $datatype->getBasetype();
        } catch (InvalidLengthException $e) {
            throw new UserException(
                sprintf(
                    'Column "%s" specified for incremental fetching must has numeric or timestamp type.',
                    $exportConfig->getIncrementalFetchingColumn()
                )
            );
        }

        if (!in_array($type, self::INCREMENTAL_TYPES, true)) {
            throw new UserException(sprintf(
                'Column "%s" specified for incremental fetching has unexpected type "%s", expected: "%s".',
                $exportConfig->getIncrementalFetchingColumn(),
                $datatype->getBasetype(),
                implode('", "', self::INCREMENTAL_TYPES),
            ));
        }
    }

    public function getMaxOfIncrementalFetchingColumn(ExportConfig $exportConfig): ?string
    {
        $sql = sprintf(
            'SELECT MAX(%s) as %s FROM %s.%s',
            $this->quote($exportConfig->getIncrementalFetchingColumn()),
            $this->quote($exportConfig->getIncrementalFetchingColumn()),
            $this->quote($exportConfig->getTable()->getSchema()),
            $this->quote($exportConfig->getTable()->getName())
        );

        try {
            $result = $this->db->query($sql)->fetchAll();
        } catch (PDOException $e) {
            throw $this->handleDbError($e, 0);
        }

        return $result ? $result[0][$exportConfig->getIncrementalFetchingColumn()] : null;
    }

    public function simpleQuery(ExportConfig $exportConfig): string
    {
        $sql = [];

        if ($exportConfig->hasColumns()) {
            $sql[] = sprintf('SELECT %s', implode(', ', array_map(
                fn(string $c) => $this->quote($c),
                $exportConfig->getColumns()
            )));
        } else {
            $sql[] = 'SELECT *';
        }

        $sql[] = sprintf(
            'FROM %s.%s',
            $this->quote($exportConfig->getTable()->getSchema()),
            $this->quote($exportConfig->getTable()->getName())
        );

        if ($exportConfig->isIncrementalFetching() && isset($this->state['lastFetchedRow'])) {
            $sql[] = sprintf(
            // intentionally ">=" last row should be included, it is handled by storage deduplication process
                'WHERE %s >= %s',
                $this->quote($exportConfig->getIncrementalFetchingColumn()),
                $this->db->quote((string) $this->state['lastFetchedRow'])
            );
        }

        if ($exportConfig->isIncrementalFetching()) {
            $sql[] = sprintf('ORDER BY %s', $this->quote($exportConfig->getIncrementalFetchingColumn()));
        }

        if ($exportConfig->hasIncrementalFetchingLimit()) {
            $sql[] = sprintf('LIMIT %d', $exportConfig->getIncrementalFetchingLimit());
        }

        return implode(' ', $sql);
    }

    private function quote(string $obj): string
    {
        return "`{$obj}`";
    }
}
