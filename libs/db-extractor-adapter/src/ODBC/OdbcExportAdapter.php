<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\ODBC;

use Psr\Log\LoggerInterface;
use Keboola\DbExtractor\Adapter\BaseExportAdapter;
use Keboola\DbExtractor\Adapter\Query\QueryFactory;

class OdbcExportAdapter extends BaseExportAdapter
{
    public function __construct(
        LoggerInterface $logger,
        OdbcConnection $connection,
        QueryFactory $simpleQueryFactory,
        string $dataDir,
        array $state
    ) {
        parent::__construct($logger, $connection, $simpleQueryFactory, $dataDir, $state);
    }

    public function getName(): string
    {
        return 'ODBC';
    }
}
