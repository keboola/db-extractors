<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\CommonExceptions\UserExceptionInterface;
use Keboola\DbExtractor\Adapter\Connection\DbConnection;
use Keboola\DbExtractor\Adapter\Exception\UserRetriedException;
use Keboola\DbExtractor\Adapter\ExportAdapter;
use Keboola\DbExtractor\Adapter\Query\QueryFactory;
use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Throwable;

class OracleExportAdapter implements ExportAdapter
{

    protected DbConnection $connection;

    protected QueryFactory $simpleQueryFactory;

    protected OracleJavaExportWrapper $exportWrapper;

    public function __construct(
        QueryFactory $simpleQueryFactory,
        OracleDbConnection $connection,
        OracleJavaExportWrapper $exportWrapper
    ) {
        $this->simpleQueryFactory = $simpleQueryFactory;
        $this->connection = $connection;
        $this->exportWrapper = $exportWrapper;
    }

    public function export(ExportConfig $exportConfig, string $csvFilePath): ExportResult
    {
        $query = $exportConfig->hasQuery() ?
            rtrim($exportConfig->getQuery(), ' ;') :
            $this->createSimpleQuery($exportConfig)
        ;

        // Export
        try {
            return $this->exportWrapper->export(
                $query,
                $exportConfig->getMaxRetries(),
                $csvFilePath,
                $exportConfig->hasQuery()
            );
        } catch (Throwable $e) {
            $logPrefix = $exportConfig->hasConfigName() ?
                $exportConfig->getConfigName() : $exportConfig->getOutputTable();
            throw $this->handleDbError($e, $exportConfig->getMaxRetries(), $query, $logPrefix);
        }
    }

    protected function createSimpleQuery(ExportConfig $exportConfig): string
    {
        return $this->simpleQueryFactory->create($exportConfig, $this->connection);
    }

    public function getName(): string
    {
        return 'Oracle';
    }

    protected function handleDbError(
        Throwable $e,
        int $maxRetries,
        string $query,
        ?string $outputTable = null
    ): UserExceptionInterface {
        $message = $outputTable ? sprintf('[%s]: ', $outputTable) : '';
        $message .= sprintf('DB query "%s" failed: %s', $query, $e->getMessage());

        // Retry mechanism can be disabled
        if ($maxRetries > 1) {
            $message .= sprintf(' Tried %d times.', $maxRetries);
        }

        return new UserRetriedException($maxRetries, $message, 0, $e);
    }
}
