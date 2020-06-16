<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Component\JsonHelper;
use Keboola\DbExtractor\DbRetryProxy;
use Keboola\DbExtractor\Exception\OracleJavaExportException;
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

    private array $dbParams;

    public function __construct(LoggerInterface $logger, string $dataDir, array $dbParams)
    {
        $this->logger = $logger;
        $this->dataDir = $dataDir;
        $dbParams['port'] = (string) $dbParams['port'];
        $this->dbParams = $dbParams;
    }

    public function testConnection(): void
    {
        $this->runAction(
            'testConnection',
            $this->writeTestConnectionConfig(),
            [],
            DbRetryProxy::DEFAULT_MAX_TRIES,
            'Failed connecting to DB'
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
                DbRetryProxy::DEFAULT_MAX_TRIES,
                'Error fetching table listing'
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
        bool $includeHeader
    ): int {
        $process = $this->runAction(
            'export',
            $this->writeExportConfig($query, $outputFile),
            [var_export($includeHeader, true)],
            $maxRetries,
            'Export process failed'
        );

        $output = $process->getOutput();
        $this->logger->info($output); // log the process output
        $fetchedPos = (int) strpos($output, 'Fetched');
        $rowCountStr = substr($output, $fetchedPos, strpos($output, 'rows in') - $fetchedPos);
        $linesWritten = (int) filter_var($rowCountStr, FILTER_SANITIZE_NUMBER_INT);
        return $linesWritten;
    }

    private function runAction(
        string $action,
        string $configFile,
        array $args,
        int $maxRetries,
        string $errorMsgPrefix
    ): Process {
        try {
            return $this->runCommand(
                $this->getCmd($action, $configFile, $args),
                $maxRetries,
                $errorMsgPrefix
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
                    $process->getErrorOutput()
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
        ], $args);
    }

    private function writeTestConnectionConfig(): string
    {
        return $this->writeConfig('test connection', [
            'parameters' => [
                'db' => $this->dbParams,
            ],
        ]);
    }

    /***
     * @param array|InputTable[] $whitelist
     */
    private function writeTestGetTablesConfig(array $whitelist, bool $loadColumns, string $outputFile): string
    {
        return $this->writeConfig('get tables', [
            'parameters' => [
                'db' => $this->dbParams,
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
                'db' => $this->dbParams,
                'query' => $query,
                'outputFile' => $outputFile,
            ],
        ]);
    }

    private function writeConfig(string $configDesc, array $config): string
    {
        $configPath = $this->dataDir . '/javaConfig.json';
        JsonHelper::writeFile($configPath, $config);

        $this->logger->info(sprintf(
            'Created "%s" configuration for "java-oracle-exporter" tool, host: "%s", port: "%d".',
            $configDesc,
            $this->dbParams['host'],
            $this->dbParams['port'],
        ));

        return $configPath;
    }
}
