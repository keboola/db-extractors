<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\PDO;

use Psr\Log\LoggerInterface;
use Keboola\DbExtractor\Adapter\BaseExportAdapter;
use Keboola\DbExtractor\Adapter\Query\SimpleQueryFactory;

class PdoExportAdapter extends BaseExportAdapter
{
    public function __construct(
        LoggerInterface $logger,
        PdoConnection $connection,
        SimpleQueryFactory $simpleQueryFactory,
        string $dataDir,
        array $state
    ) {
        parent::__construct($logger, $connection, $simpleQueryFactory, $dataDir, $state);
    }

    public function getName(): string
    {
        return 'PDO';
    }
}
