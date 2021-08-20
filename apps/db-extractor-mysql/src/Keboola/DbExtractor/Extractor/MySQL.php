<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\MySQL as MysqlDatatype;
use Keboola\DbExtractor\Adapter\ExportAdapter;
use Keboola\DbExtractor\Adapter\Metadata\MetadataProvider;
use Keboola\DbExtractor\Adapter\PDO\PdoExportAdapter;
use Keboola\DbExtractor\Adapter\Query\DefaultQueryFactory;
use Keboola\DbExtractor\Adapter\ResultWriter\DefaultResultWriter;
use Keboola\DbExtractor\Configuration\ValueObject\MysqlDatabaseConfig;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\TableResultFormat\Exception\ColumnNotFoundException;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Keboola\Temp\Temp;
use PDO;
use PDOException;

class MySQL extends BaseExtractor
{
    public const INCREMENTAL_TYPES = ['INTEGER', 'NUMERIC', 'FLOAT', 'TIMESTAMP'];

    protected ?string $database = null;

    protected MySQLDbConnection $connection;

    public function createMetadataProvider(): MetadataProvider
    {
        return new MySQLMetadataProvider($this->connection, $this->database);
    }

    protected function createExportAdapter(): ExportAdapter
    {
        $resultWriter = new DefaultResultWriter($this->state);
        $simpleQueryFactory = new DefaultQueryFactory($this->state);
        return new PdoExportAdapter(
            $this->logger,
            $this->connection,
            $simpleQueryFactory,
            $resultWriter,
            $this->dataDir,
            $this->state
        );
    }

    public function createConnection(DatabaseConfig $databaseConfig): void
    {
        if (!($databaseConfig instanceof MysqlDatabaseConfig)) {
            throw new ApplicationException('MysqlDatabaseConfig expected.');
        }

        // Set database
        if ($databaseConfig->hasDatabase()) {
            $this->database = $databaseConfig->getDatabase();
        }

        // Log SSL status
        if ($databaseConfig->hasSSLConnection()) {
            $this->logger->info('SSL enabled.');
        }

        // Create connection
        $connectMaxTries = $this->isSyncAction() ? 1 : MySQLDbConnection::CONNECT_MAX_RETRIES;
        $this->connection = MySQLDbConnectionFactory::create($databaseConfig, $this->logger, $connectMaxTries);

        // Check SSL
        if ($databaseConfig->hasSSLConnection()) {
            $status = $this->connection
                ->query("SHOW STATUS LIKE 'Ssl_cipher';")
                ->fetch();

            if (empty($status['Value'])) {
                throw new UserException('Connection is not encrypted');
            } else {
                $this->logger->info('Using SSL cipher: ' . $status['Value']);
            }
        }

        // Check network compression
        if ($databaseConfig->isNetworkCompressionEnabled()) {
            $status = $this->connection
                ->query("SHOW SESSION STATUS LIKE 'Compression';")
                ->fetch();

            if (empty($status['Value']) || $status['Value'] !== 'ON') {
                throw new UserException('Network communication is not compressed.');
            } else {
                $this->logger->info('Using network compression.');
            }
        }
    }

    public function testConnection(): void
    {
        $this->connection->testConnection();
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

        $result = $this->connection->query($sql)->fetchAll();
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
                $this->connection->quote((string) $this->state['lastFetchedRow'])
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

    protected function createDatabaseConfig(array $data): DatabaseConfig
    {
        return MysqlDatabaseConfig::fromArray($data);
    }

    private function quote(string $obj): string
    {
        return "`{$obj}`";
    }
}
