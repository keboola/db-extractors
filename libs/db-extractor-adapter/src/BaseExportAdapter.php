<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter;

use Throwable;
use Psr\Log\LoggerInterface;
use Keboola\CommonExceptions\UserExceptionInterface;
use Keboola\DbExtractor\Adapter\Exception\ApplicationException;
use Keboola\DbExtractor\Adapter\Exception\UserRetriedException;
use Keboola\DbExtractor\Adapter\ValueObject\QueryResult;
use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\Csv\Exception as CsvException;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbExtractor\Adapter\Connection\DbConnection;
use Keboola\DbExtractor\Adapter\Query\QueryFactory;

abstract class BaseExportAdapter implements ExportAdapter
{
    protected LoggerInterface $logger;

    protected DbConnection $connection;

    protected QueryFactory $simpleQueryFactory;

    protected string $dataDir;

    protected array $state;

    public function __construct(
        LoggerInterface $logger,
        DbConnection $connection,
        QueryFactory $simpleQueryFactory,
        string $dataDir,
        array $state
    ) {
        $this->logger = $logger;
        $this->connection = $connection;
        $this->simpleQueryFactory = $simpleQueryFactory;
        $this->dataDir = $dataDir;
        $this->state = $state;
    }

    public function export(ExportConfig $exportConfig, string $csvFilePath): ExportResult
    {
        $query = $exportConfig->hasQuery() ? $exportConfig->getQuery() : $this->createSimpleQuery($exportConfig);

        try {
            return $this->connection->queryAndProcess(
                $query,
                $exportConfig->getMaxRetries(),
                function (QueryResult $result) use ($exportConfig, $csvFilePath) {
                    $resultWriter = new QueryResultCsvWriter($this->state);
                    return $resultWriter->writeToCsv($result, $exportConfig, $csvFilePath);
                }
            );
        } catch (CsvException $e) {
            throw new ApplicationException('Failed writing CSV File: ' . $e->getMessage(), $e->getCode(), $e);
        } catch (UserExceptionInterface $e) {
            throw $this->handleDbError($e, $exportConfig->getMaxRetries(), $exportConfig->getOutputTable());
        }
    }

    protected function createSimpleQuery(ExportConfig $exportConfig): string
    {
        return $this->simpleQueryFactory->create($exportConfig, $this->connection);
    }

    protected function handleDbError(Throwable $e, int $maxRetries, ?string $outputTable = null): UserExceptionInterface
    {
        $message = $outputTable ? sprintf('[%s]: ', $outputTable) : '';
        $message .= sprintf('DB query failed: %s', $e->getMessage());

        // Retry mechanism can be disabled
        if ($maxRetries > 1) {
            $message .= sprintf(' Tried %d times.', $maxRetries);
        }

        return new UserRetriedException($message, 0, $e);
    }
}
