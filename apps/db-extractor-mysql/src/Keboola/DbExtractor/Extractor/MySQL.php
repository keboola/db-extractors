<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use PDO;
use PDOException;
use Throwable;
use Keboola\DbExtractor\Exception\ApplicationException;
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
    public const INCREMENT_TYPE_NUMERIC = 'numeric';
    public const INCREMENT_TYPE_TIMESTAMP = 'timestamp';
    public const NUMERIC_BASE_TYPES = ['INTEGER', 'NUMERIC', 'FLOAT'];

    protected ?string $database = null;

    protected string $incrementalFetchingColType;

    public function getMetadataProvider(): MetadataProvider
    {
        return new MySQLMetadataProvider($this->db, $this->database);
    }

    private function createSSLFile(string $sslCa, Temp $temp): string
    {
        $filename = $temp->createTmpFile('ssl');
        file_put_contents((string) $filename, $sslCa);
        return (string) realpath((string) $filename);
    }

    public function createConnection(array $params): PDO
    {
        $isSsl = false;
        $isCompression = !empty($params['networkCompression']) ? true :false;

        $options = [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION, // convert errors to PDOExceptions
            PDO::MYSQL_ATTR_COMPRESS => $isCompression, // network compression
        ];

        // ssl encryption
        if (!empty($params['ssl']) && !empty($params['ssl']['enabled'])) {
            $ssl = $params['ssl'];

            $temp = new Temp(getenv('APP_NAME') ? (string) getenv('APP_NAME') : 'ex-db-mysql');

            if (!empty($ssl['key'])) {
                $options[PDO::MYSQL_ATTR_SSL_KEY] = $this->createSSLFile($ssl['key'], $temp);
                $isSsl = true;
            }
            if (!empty($ssl['cert'])) {
                $options[PDO::MYSQL_ATTR_SSL_CERT] = $this->createSSLFile($ssl['cert'], $temp);
                $isSsl = true;
            }
            if (!empty($ssl['ca'])) {
                $options[PDO::MYSQL_ATTR_SSL_CA] = $this->createSSLFile($ssl['ca'], $temp);
                $isSsl = true;
            }
            if (!empty($ssl['cipher'])) {
                $options[PDO::MYSQL_ATTR_SSL_CIPHER] = $ssl['cipher'];
            }
            if (isset($ssl['verifyServerCert']) && $ssl['verifyServerCert'] === false) {
                $options[PDO::MYSQL_ATTR_SSL_VERIFY_SERVER_CERT] = false;
            }

            $options[PDO::MYSQL_ATTR_SSL_CIPHER] = self::SSL_CIPHER_CONFIG;
        }

        foreach (['host', 'user', '#password'] as $r) {
            if (!array_key_exists($r, $params)) {
                throw new UserException(sprintf('Parameter %s is missing.', $r));
            }
        }

        $port = !empty($params['port']) ? $params['port'] : '3306';

        $dsn = sprintf(
            'mysql:host=%s;port=%s;charset=utf8',
            $params['host'],
            $port
        );

        if (isset($params['database'])) {
            $dsn = sprintf(
                'mysql:host=%s;port=%s;dbname=%s;charset=utf8',
                $params['host'],
                $port,
                $params['database']
            );
            $this->database = $params['database'];
        }

        $this->logger->info("Connecting to DSN '" . $dsn . "' " . ($isSsl ? 'Using SSL' : ''));

        try {
            $pdo = new PDO($dsn, $params['user'], $params['#password'], $options);
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
        if ($this->database && $this->database !== $exportConfig->getTable()->getSchema()) {
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
                    'Column [%s] specified for incremental fetching was not found in the table',
                    $exportConfig->getIncrementalFetchingColumn()
                )
            );
        }

        try {
            $datatype = new MysqlDatatype($column->getType());
            if (in_array($datatype->getBasetype(), self::NUMERIC_BASE_TYPES)) {
                $this->incrementalFetchingColType = self::INCREMENT_TYPE_NUMERIC;
            } else if ($datatype->getBasetype() === 'TIMESTAMP') {
                $this->incrementalFetchingColType = self::INCREMENT_TYPE_TIMESTAMP;
            } else {
                throw new UserException('invalid incremental fetching column type');
            }
        } catch (InvalidLengthException | UserException $exception) {
            throw new UserException(
                sprintf(
                    'Column [%s] specified for incremental fetching is not a numeric or timestamp type column',
                    $exportConfig->getIncrementalFetchingColumn()
                )
            );
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
        $result = $this->db->query($sql)->fetchAll();
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
            if ($this->incrementalFetchingColType === self::INCREMENT_TYPE_NUMERIC) {
                $sql[] = sprintf(
                    // intentionally ">=" last row should be included, it is handled by storage deduplication process
                    'WHERE %s >= %d',
                    $this->quote($exportConfig->getIncrementalFetchingColumn()),
                    (int) $this->state['lastFetchedRow']
                );
            } else if ($this->incrementalFetchingColType === self::INCREMENT_TYPE_TIMESTAMP) {
                $sql[] = sprintf(
                    // intentionally ">=" last row should be included, it is handled by storage deduplication process
                    'WHERE %s >= \'%s\'',
                    $this->quote($exportConfig->getIncrementalFetchingColumn()),
                    $this->state['lastFetchedRow']
                );
            } else {
                throw new ApplicationException(
                    sprintf('Unknown incremental fetching column type %s', $this->incrementalFetchingColType)
                );
            }
        }

        $sql[] = sprintf('ORDER BY %s', $this->quote($exportConfig->getIncrementalFetchingColumn()));

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
