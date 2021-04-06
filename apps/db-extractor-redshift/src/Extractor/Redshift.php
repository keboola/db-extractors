<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\Redshift as RedshiftDatatype;
use Keboola\DbExtractor\Adapter\ExportAdapter;
use Keboola\DbExtractor\Adapter\Metadata\MetadataProvider;
use Keboola\DbExtractor\Adapter\PDO\PdoExportAdapter;
use Keboola\DbExtractor\Adapter\ResultWriter\DefaultResultWriter;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use PDO;

class Redshift extends BaseExtractor
{
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

    protected RedshiftPdoConnection $connection;

    protected RedshiftQueryFactory $queryFactory;

    /**
     * @return RedshiftMetadataProvider
     */
    public function getMetadataProvider(): MetadataProvider
    {
        return new RedshiftMetadataProvider($this->connection);
    }

    protected function getQueryFactory(): RedshiftQueryFactory
    {
        if (!isset($this->queryFactory)) {
            $this->queryFactory = new RedshiftQueryFactory($this->state);
        }

        return $this->queryFactory;
    }

    protected function createExportAdapter(): ExportAdapter
    {
        $resultWriter = new DefaultResultWriter($this->state);
        return new PdoExportAdapter(
            $this->logger,
            $this->connection,
            $this->getQueryFactory(),
            $resultWriter,
            $this->dataDir,
            $this->state
        );
    }

    public function createConnection(DatabaseConfig $databaseConfig): void
    {
        $dsn = sprintf(
            'pgsql:dbname=%s;port=%s;host=%s',
            $databaseConfig->getDatabase(),
            $databaseConfig->getPort(),
            $databaseConfig->getHost()
        );

        $this->connection = new RedshiftPdoConnection(
            $this->logger,
            $dsn,
            $databaseConfig->getUsername(),
            $databaseConfig->getPassword(),
            [],
            function (PDO $pdo): void {
                $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
            }
        );
    }

    public function testConnection(): void
    {
        $this->connection->testConnection();
    }

    public function validateIncrementalFetching(ExportConfig $exportConfig): void
    {
        $query = sprintf(
            'SELECT * FROM information_schema.columns 
                            WHERE table_schema = %s AND table_name = %s AND column_name = %s',
            $this->connection->quote($exportConfig->getTable()->getSchema()),
            $this->connection->quote($exportConfig->getTable()->getName()),
            $this->connection->quote($exportConfig->getIncrementalFetchingConfig()->getColumn())
        );
        $columns = $this->connection->query($query)->fetchAll();
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
                $this
                    ->getQueryFactory()
                    ->setIncrementalFetchingColType(self::INCREMENT_TYPE_NUMERIC)
                ;
            } elseif (in_array($datatype->getBasetype(), self::TIMESTAMP_BASE_TYPES)) {
                $this
                    ->getQueryFactory()
                    ->setIncrementalFetchingColType(self::INCREMENT_TYPE_TIMESTAMP)
                ;
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

    public function getMaxOfIncrementalFetchingColumn(ExportConfig $exportConfig): ?string
    {
        $sql = 'SELECT MAX(%s) as %s FROM %s.%s';
        $fullsql = sprintf(
            $sql,
            $this->connection->quoteIdentifier($exportConfig->getIncrementalFetchingConfig()->getColumn()),
            $this->connection->quoteIdentifier($exportConfig->getIncrementalFetchingConfig()->getColumn()),
            $this->connection->quoteIdentifier($exportConfig->getTable()->getSchema()),
            $this->connection->quoteIdentifier($exportConfig->getTable()->getName())
        );
        $result = $this->connection->query($fullsql)->fetchAll();
        return $result ? (string) $result[0][$exportConfig->getIncrementalFetchingColumn()] : null;
    }
}
