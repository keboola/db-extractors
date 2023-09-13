<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Adapter\ExportAdapter;
use Keboola\DbExtractor\Adapter\Metadata\MetadataProvider;
use Keboola\DbExtractor\Configuration\OracleDatabaseConfig;
use Keboola\DbExtractor\Manifest\ManifestGenerator;
use Keboola\DbExtractor\TableResultFormat\Metadata\Manifest\DefaultManifestSerializer;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbExtractor\TableResultFormat\Exception\ColumnNotFoundException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\GenericStorage;

class Oracle extends BaseExtractor
{
    public const INCREMENT_TYPE_NUMERIC = 'numeric';
    public const INCREMENT_TYPE_TIMESTAMP = 'timestamp';
    public const INCREMENT_TYPE_DATE = 'date';
    public const NUMERIC_BASE_TYPES = ['INTEGER', 'NUMERIC', 'FLOAT'];

    private OracleJavaExportWrapper $exportWrapper;

    private OracleQueryFactory $queryFactory;

    private OracleDbConnection $connection;

    public function createConnection(DatabaseConfig $databaseConfig): void
    {
        // OracleJavaExportWrapper must be created after parent constructor,
        // ... because dbParameters are modified by SSH tunnel.
        $this->exportWrapper = new OracleJavaExportWrapper($this->logger, $this->dataDir, $databaseConfig);
    }

    protected function createMetadataProvider(): MetadataProvider
    {
        return new OracleMetadataProvider($this->exportWrapper);
    }

    protected function createManifestGenerator(): ManifestGenerator
    {
        return new OracleManifestGenerator(
            $this->getMetadataProvider(),
            new DefaultManifestSerializer()
        );
    }

    protected function createExportAdapter(): ExportAdapter
    {
        $this->queryFactory = new OracleQueryFactory($this->state);
        $this->connection = new OracleDbConnection();
        return new OracleExportAdapter(
            $this->queryFactory,
            $this->connection,
            $this->exportWrapper
        );
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
            throw new UserException(sprintf(
                'Column "%s" specified for incremental fetching was not found in the table.',
                $exportConfig->getIncrementalFetchingColumn()
            ));
        }

        $this->checkForNulls($exportConfig);

        try {
            $datatype = new GenericStorage($column->getType());
            if (in_array($datatype->getBasetype(), self::NUMERIC_BASE_TYPES)) {
                $incrementalFetchingType = self::INCREMENT_TYPE_NUMERIC;
            } else if ($datatype->getBasetype() === 'TIMESTAMP') {
                $incrementalFetchingType = self::INCREMENT_TYPE_TIMESTAMP;
            } else if ($datatype->getBasetype() === 'DATE') {
                $incrementalFetchingType = self::INCREMENT_TYPE_DATE;
            } else {
                throw new UserException('invalid incremental fetching column type');
            }
        } catch (InvalidLengthException | UserException $exception) {
            throw new UserException(
                sprintf(
                    'Column "%s" specified for incremental fetching is not a numeric or timestamp type column.',
                    $column->getName()
                )
            );
        }
        $this->queryFactory->setIncrementalFetchingColType($incrementalFetchingType);
    }

    public function getMaxOfIncrementalFetchingColumn(ExportConfig $exportConfig): ?string
    {
        $outputFile = $this->getOutputFilename('last_row');
        $this->exportWrapper->export(
            $this->queryFactory->createLastRowQuery($exportConfig, $this->connection),
            $exportConfig->getMaxRetries(),
            $outputFile,
            false
        );

        $value = json_decode((string) file_get_contents($outputFile));
        unlink($outputFile);
        return $value;
    }

    public function testConnection(): void
    {
        $this->exportWrapper->testConnection();
    }

    protected function createDatabaseConfig(array $data): DatabaseConfig
    {
        return OracleDatabaseConfig::fromArray($data);
    }

    protected function canFetchMaxIncrementalValueSeparately(ExportConfig $exportConfig): bool
    {
        return
            !$exportConfig->hasQuery() &&
            $exportConfig->isIncrementalFetching();
    }

    protected function checkForNulls(ExportConfig $exportConfig): void
    {
        $outputFile = $this->getOutputFilename('nullValues');
        $query = $this->queryFactory->createCheckNullsQuery($exportConfig, $this->connection);
        $this->exportWrapper->export($query, $exportConfig->getMaxRetries(), $outputFile, false);

        $nullCount = json_decode((string) file_get_contents($outputFile));
        unlink($outputFile);

        if ((int) $nullCount > 0) {
            throw new UserException(sprintf(
                'Cannot set incremental fetching on nullable column "%s".',
                $exportConfig->getIncrementalFetchingColumn()
            ));
        }
    }
}
