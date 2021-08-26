<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\PDO;

use Keboola\DbExtractor\Adapter\BaseExportAdapter;
use Keboola\DbExtractor\Adapter\Query\QueryFactory;
use Keboola\DbExtractor\Adapter\ResultWriter\ResultWriter;
use Psr\Log\LoggerInterface;

class PdoExportAdapter extends BaseExportAdapter
{
    public function __construct(
        LoggerInterface $logger,
        PdoConnection $connection,
        QueryFactory $simpleQueryFactory,
        ResultWriter $resultWriter,
        string $dataDir,
        array $state
    ) {
        parent::__construct($logger, $connection, $simpleQueryFactory, $resultWriter, $dataDir, $state);
    }

    public function getName(): string
    {
        return 'PDO';
    }
}
