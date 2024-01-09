<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter;

use Keboola\CommonExceptions\UserExceptionInterface;
use Keboola\Csv\Exception as CsvException;
use Keboola\DbExtractor\Adapter\Connection\DbConnection;
use Keboola\DbExtractor\Adapter\Exception\ApplicationException;
use Keboola\DbExtractor\Adapter\Exception\UserException;
use Keboola\DbExtractor\Adapter\Exception\UserRetriedException;
use Keboola\DbExtractor\Adapter\Query\QueryFactory;
use Keboola\DbExtractor\Adapter\ResultWriter\ResultWriter;
use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractor\Adapter\ValueObject\QueryResult;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Psr\Log\LoggerInterface;
use Throwable;

abstract class BaseExportAdapter implements ExportAdapter
{
    protected LoggerInterface $logger;

    protected DbConnection $connection;

    protected QueryFactory $simpleQueryFactory;

    protected ResultWriter $resultWriter;

    protected string $dataDir;

    protected array $state;

    public function __construct(
        LoggerInterface $logger,
        DbConnection $connection,
        QueryFactory $simpleQueryFactory,
        ResultWriter $resultWriter,
        string $dataDir,
        array $state,
    ) {
        $this->logger = $logger;
        $this->connection = $connection;
        $this->simpleQueryFactory = $simpleQueryFactory;
        $this->resultWriter = $resultWriter;
        $this->dataDir = $dataDir;
        $this->state = $state;
    }

    public function export(ExportConfig $exportConfig, string $csvFilePath): ExportResult
    {
        $query = $exportConfig->hasQuery() ? $exportConfig->getQuery() : $this->createSimpleQuery($exportConfig);

        try {
            return $this->queryAndProcess(
                $query,
                $exportConfig->getMaxRetries(),
                function (QueryResult $result) use ($exportConfig, $csvFilePath) {
                    return $this->resultWriter->writeToCsv($result, $exportConfig, $csvFilePath);
                },
            );
        } catch (CsvException $e) {
            throw new ApplicationException('Failed writing CSV File: ' . $e->getMessage(), $e->getCode(), $e);
        } catch (UserExceptionInterface $e) {
            throw $this->handleDbError($e, $exportConfig->getMaxRetries(), $exportConfig->getOutputTable());
        }
    }

    protected function queryAndProcess(string $query, int $maxRetries, callable $processor): ExportResult
    {
        return $this->connection->queryAndProcess($query, $maxRetries, $processor);
    }

    protected function createSimpleQuery(ExportConfig $exportConfig): string
    {
        return $this->simpleQueryFactory->create($exportConfig, $this->connection);
    }

    protected function handleDbError(Throwable $e, int $maxRetries, ?string $outputTable = null): UserExceptionInterface
    {
        $message = $outputTable ? sprintf('[%s]: ', $outputTable) : '';
        $message .= sprintf('DB query failed: %s', $e->getMessage());

        // Retry mechanism can be disabled or modified, ... we need to print real count
        if ($e instanceof UserRetriedException && $e->getTryCount() > 1) {
            $message .= sprintf(' Tried %d times.', $maxRetries);
            return new UserRetriedException($e->getTryCount(), $message, 0, $e);
        }

        return new UserException($message, 0, $e);
    }
}
