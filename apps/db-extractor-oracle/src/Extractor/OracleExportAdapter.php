<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Adapter\BaseExportAdapter;
use Keboola\DbExtractor\Adapter\Query\QueryFactory;
use Keboola\DbExtractor\Adapter\ResultWriter\ResultWriter;
use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Psr\Log\LoggerInterface;
use Throwable;

class OracleExportAdapter extends BaseExportAdapter
{

    protected OracleJavaExportWrapper $exportWrapper;

    public function __construct(
        LoggerInterface $logger,
        QueryFactory $simpleQueryFactory,
        ResultWriter $resultWriter,
        OracleDbConnection $connection,
        OracleJavaExportWrapper $exportWrapper,
        string $dataDir,
        array $state
    ) {
        $this->logger = $logger;
        $this->simpleQueryFactory = $simpleQueryFactory;
        $this->resultWriter = $resultWriter;
        $this->dataDir = $dataDir;
        $this->state = $state;
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
            throw $this->handleDbError($e, $exportConfig->getMaxRetries(), $logPrefix);
        }
    }

    public function getName(): string
    {
        return 'Oracle';
    }
}
