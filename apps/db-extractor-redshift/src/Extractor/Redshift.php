<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\Redshift as RedshiftDatatype;
use Keboola\DbExtractor\DbRetryProxy;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Traits\QuoteTrait;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use PDO;
use Throwable;

class Redshift extends BaseExtractor
{
    use QuoteTrait;

    public const INCREMENT_TYPE_NUMERIC = 'numeric';
    public const INCREMENT_TYPE_TIMESTAMP = 'timestamp';
    public const NUMERIC_BASE_TYPES = ['INTEGER', 'NUMERIC', 'FLOAT'];
    public const TIMESTAMP_BASE_TYPES = [
        'DATE',
        'TIMESTAMP',
        'TIMESTAMP WITHOUT TIME ZONE',
        'TIMESTAMPTZ',
        'TIMESTAMP WITH TIME ZONE',
    ];

    private string $incrementalFetchingColType;

    public function getMetadataProvider(): MetadataProvider
    {
        return new RedshiftMetadataProvider($this->db);
    }

    public function createConnection(DatabaseConfig $databaseConfig): PDO
    {
        $dsn = sprintf(
            'pgsql:dbname=%s;port=%s;host=%s',
            $databaseConfig->getDatabase(),
            $databaseConfig->getPort(),
            $databaseConfig->getHost()
        );
        $pdo = new PDO(
            $dsn,
            $databaseConfig->getUsername(),
            $databaseConfig->getPassword()
        );
        $this->logger->info(sprintf('Connecting to %s', $dsn));
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        return $pdo;
    }

    public function testConnection(): void
    {
        $this->db->query('SELECT 1');
    }


    public function validateIncrementalFetching(ExportConfig $exportConfig): void
    {
        $query = sprintf(
            'SELECT * FROM information_schema.columns 
                            WHERE table_schema = %s AND table_name = %s AND column_name = %s',
            $this->db->quote($exportConfig->getTable()->getSchema()),
            $this->db->quote($exportConfig->getTable()->getName()),
            $this->db->quote($exportConfig->getIncrementalFetchingConfig()->getColumn())
        );
        $columns = $this->runRetriableQuery($query, 'Error get column info');
        if (count($columns) === 0) {
            throw new UserException(
                sprintf(
                    'Column [%s] specified for incremental fetching was not found in the table',
                    $exportConfig->getIncrementalFetchingConfig()->getColumn()
                )
            );
        }

        try {
            $datatype = new RedshiftDatatype(strtoupper($columns[0]['data_type']));
            if (in_array($datatype->getBasetype(), self::NUMERIC_BASE_TYPES)) {
                $this->incrementalFetchingColType = self::INCREMENT_TYPE_NUMERIC;
            } elseif (in_array($datatype->getBasetype(), self::TIMESTAMP_BASE_TYPES)) {
                $this->incrementalFetchingColType = self::INCREMENT_TYPE_TIMESTAMP;
            } else {
                throw new UserException('invalid incremental fetching column type');
            }
        } catch (InvalidLengthException | UserException $exception) {
            throw new UserException(
                sprintf(
                    'Column [%s] specified for incremental fetching is not a numeric or timestamp type column',
                    $exportConfig->getIncrementalFetchingConfig()->getColumn()
                )
            );
        }
    }


    public function simpleQuery(ExportConfig $exportConfig): string
    {
        $query = [];
        if ($exportConfig->hasColumns()) {
            $query[] = sprintf(
                'SELECT %s FROM %s.%s',
                implode(', ', array_map(function ($column) {
                    return $this->quoteIdentifier($column);
                }, $exportConfig->getColumns())),
                $this->quoteIdentifier($exportConfig->getTable()->getSchema()),
                $this->quoteIdentifier($exportConfig->getTable()->getName())
            );
        } else {
            $query[] = sprintf(
                'SELECT * FROM %s.%s',
                $this->quoteIdentifier($exportConfig->getTable()->getSchema()),
                $this->quoteIdentifier($exportConfig->getTable()->getName())
            );
        }

        if ($exportConfig->isIncrementalFetching()) {
            if (isset($this->state['lastFetchedRow'])) {
                if ($this->incrementalFetchingColType === self::INCREMENT_TYPE_NUMERIC) {
                    $lastFetchedRow = $this->state['lastFetchedRow'];
                } else {
                    $lastFetchedRow = $this->db->quote((string) $this->state['lastFetchedRow']);
                }
                $query[] = sprintf(
                    'WHERE %s >= %s',
                    $this->quoteIdentifier($exportConfig->getIncrementalFetchingConfig()->getColumn()),
                    $lastFetchedRow
                );
            }
            $query[] = sprintf(
                'ORDER BY %s',
                $this->quoteIdentifier($exportConfig->getIncrementalFetchingConfig()->getColumn())
            );

            if ($exportConfig->getIncrementalFetchingConfig()->hasLimit()) {
                $query[] = sprintf(
                    'LIMIT %d',
                    $exportConfig->getIncrementalFetchingConfig()->getLimit()
                );
            }
        }

        return implode(' ', $query);
    }

    public function getMaxOfIncrementalFetchingColumn(ExportConfig $exportConfig): ?string
    {
        $sql = 'SELECT MAX(%s) as %s FROM %s.%s';
        $fullsql = sprintf(
            $sql,
            $this->quoteIdentifier($exportConfig->getIncrementalFetchingConfig()->getColumn()),
            $this->quoteIdentifier($exportConfig->getIncrementalFetchingConfig()->getColumn()),
            $this->quoteIdentifier($exportConfig->getTable()->getSchema()),
            $this->quoteIdentifier($exportConfig->getTable()->getName())
        );
        $result = $this->runRetriableQuery($fullsql, 'Error fetching maximum value');
        if (count($result) > 0) {
            return (string) $result[0][$exportConfig->getIncrementalFetchingConfig()->getColumn()];
        }
        return null;
    }

    private function runRetriableQuery(string $query, string $errorMessage = '', ?int $fetchStyle = null): array
    {
        $retryProxy = new DbRetryProxy(
            $this->logger,
            DbRetryProxy::DEFAULT_MAX_TRIES
        );
        return $retryProxy->call(function () use ($query, $fetchStyle, $errorMessage): array {
            try {
                $res = $this->db->query($query);
                if (!is_null($fetchStyle)) {
                    return $res->fetchAll($fetchStyle);
                } else {
                    return $res->fetchAll();
                }
            } catch (Throwable $e) {
                throw new UserException(
                    $errorMessage . ': ' . $e->getMessage(),
                    0,
                    $e
                );
            }
        });
    }
}
