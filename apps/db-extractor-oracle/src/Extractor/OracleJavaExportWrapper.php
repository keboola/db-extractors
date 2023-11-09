<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Component\JsonHelper;
use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractor\Configuration\OracleDatabaseConfig;
use Keboola\DbExtractor\Configuration\Serializer\OracleDatabaseConfigSerializer;
use Keboola\DbExtractor\DbRetryProxy;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\OracleJavaExportException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\InputTable;
use Psr\Log\LoggerInterface;
use Symfony\Component\Process\Process;

/**
 * PHP interface for https://github.com/keboola/java-oracle-exporter
 */
class OracleJavaExportWrapper
{
    private LoggerInterface $logger;

    private string $dataDir;

    private OracleDatabaseConfig $databaseConfig;

    private array $javaDbConfig;

    private const MAX_TRIES_TEST_CONNECTION = 1;

    private const MAX_TRIES_GET_TABLES = 1;

    public function __construct(LoggerInterface $logger, string $dataDir, DatabaseConfig $databaseConfig)
    {
        if (!($databaseConfig instanceof OracleDatabaseConfig)) {
            throw new ApplicationException('Database config must be instance of the OracleDatabaseConfig');
        }
        $this->logger = $logger;
        $this->dataDir = $dataDir;
        $this->databaseConfig = $databaseConfig;
        $this->javaDbConfig = OracleDatabaseConfigSerializer::serialize($this->logger, $this->databaseConfig);
    }

    public function testConnection(): void
    {
        $this->runAction(
            'testConnection',
            $this->writeTestConnectionConfig(),
            [],
            self::MAX_TRIES_TEST_CONNECTION,
            'Failed connecting to DB',
        );
    }

    /**
     * @param array|InputTable[] $whitelist
     */
    public function getTables(array $whitelist, bool $loadColumns): array
    {
        $outputFile = $this->dataDir . '/tables.json';
        try {
            $this->runAction(
                'getTables',
                $this->writeTestGetTablesConfig($whitelist, $loadColumns, $outputFile),
                [],
                self::MAX_TRIES_GET_TABLES,
                'Error fetching table listing',
            );

            return JsonHelper::readFile($outputFile);
        } finally {
            @unlink($outputFile);
        }
    }

    public function export(
        string $query,
        int $maxRetries,
        string $outputFile,
        bool $includeHeader,
    ): ExportResult {
        $this->logger->debug(sprintf('Running query "%s".', $query));
        $process = $this->runAction(
            'export',
            $this->writeExportConfig($query, $outputFile),
            [var_export($includeHeader, true)],
            $maxRetries,
            'Export process failed',
        );

        $output = $process->getOutput();
        foreach (explode("\n", trim($output)) as $logRow) {
            $this->logger->info($logRow); // log the process output
        }
        $fetchedPos = (int) strpos($output, 'Fetched');
        $rowCountStr = substr($output, $fetchedPos, strpos($output, 'rows in') - $fetchedPos);
        $linesWritten = (int) filter_var($rowCountStr, FILTER_SANITIZE_NUMBER_INT);
        return new ExportResult($outputFile, $linesWritten, new OracleQueryMetadata(), false, null);
    }

    private function runAction(
        string $action,
        string $configFile,
        array $args,
        int $maxRetries,
        string $errorMsgPrefix,
    ): Process {
        try {
            return $this->runCommand(
                $this->getCmd($action, $configFile, $args),
                $maxRetries,
                $errorMsgPrefix,
            );
        } finally {
            @unlink($configFile);
        }
    }

    private function runCommand(array $cmd, int $maxRetries, string $errorMsgPrefix): Process
    {
        $retryProxy = new DbRetryProxy($this->logger, $maxRetries, [OracleJavaExportException::class]);
        return $retryProxy->call(function () use ($cmd, $errorMsgPrefix): Process {
            $process = new Process($cmd);
            $process->setTimeout(null);
            $process->setIdleTimeout(null);
            $process->run();

            if (!$process->isSuccessful()) {
                throw new OracleJavaExportException(sprintf(
                    '%s: %s',
                    $errorMsgPrefix,
                    $process->getErrorOutput(),
                ));
            }

            return $process;
        });
    }

    private function getCmd(string $action, string $configFile, array $args = []): array
    {
        return array_merge([
            'java',
            '-jar',
            '/opt/table-exporter.jar',
            $action,
            $configFile,
            $this->databaseConfig->hasTnsnames() ? $this->dataDir : '',
        ], $args);
    }

    private function writeTestConnectionConfig(): string
    {
        return $this->writeConfig('test connection', [
            'parameters' => [
                'db' => $this->javaDbConfig,
            ],
        ]);
    }

    private function writeTestGetTablesConfig(array $whitelist, bool $loadColumns, string $outputFile): string
    {
        return $this->writeConfig('get tables', [
            'parameters' => [
                'db' => $this->javaDbConfig,
                'outputFile' => $outputFile,
                'tables' => array_map(function (InputTable $table) {
                    return [
                        'tableName' => $table->getName(),
                        'schema' => $table->getSchema(),
                    ];
                }, $whitelist),
                'includeColumns' => $loadColumns,
            ],
        ]);
    }

    private function writeExportConfig(string $query, string $outputFile): string
    {
        return $this->writeConfig('export', [
            'parameters' => [
                'db' => $this->javaDbConfig,
                'query' => $query,
                'outputFile' => $outputFile,
            ],
        ]);
    }

    private function writeConfig(string $configDesc, array $config): string
    {
        if ($this->databaseConfig->hasTnsnames()) {
            $this->writeTnsnames($this->databaseConfig->getTnsnames());
            $config['parameters']['db']['tnsnamesService'] = $this->getTnsnamesService(
                $this->databaseConfig->getTnsnames(),
            );
        }

        $configPath = $this->dataDir . '/javaConfig.json';
        JsonHelper::writeFile($configPath, $config);

        if ($this->databaseConfig->hasHost()) {
            $this->logger->info(sprintf(
                'Created "%s" configuration for "java-oracle-exporter" tool, host: "%s", port: "%d".',
                $configDesc,
                $this->databaseConfig->getHost(),
                $this->databaseConfig->getPort(),
            ));
        } else {
            $this->logger->info(sprintf(
                'Created "%s" configuration for "java-oracle-exporter" tool.',
                $configDesc,
            ));
        }

        return $configPath;
    }

    private function writeTnsnames(string $tnsnameContent): void
    {
        file_put_contents(
            sprintf('%s/%s', $this->dataDir, 'tnsnames.ora'),
            $tnsnameContent,
        );

        $this->logger->info('Created "tnsname.ora" file for "java-oracle-exporter" tool.');
    }

    private function getTnsnamesService(string $tnsnamesContent): string
    {
        preg_match('/\(SERVICE_NAME\s?=\s?(.+[^\)])\)/i', $tnsnamesContent, $match);

        if (!isset($match[1])) {
            throw  new UserException('Missing "SERVICE_NAME" in the tnsnames.');
        }

        return (string) $match[1];
    }
}
