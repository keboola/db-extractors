<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\Tests\ODBC;

use Keboola\DbExtractor\Adapter\ODBC\OdbcExportAdapter;
use Keboola\DbExtractor\Adapter\Query\DefaultQueryFactory;
use Keboola\DbExtractor\Adapter\Query\QueryFactory;
use Keboola\DbExtractor\Adapter\Tests\AbstractExportAdapterTest;
use Keboola\DbExtractor\Adapter\Tests\Traits\OdbcCreateConnectionTrait;
use PHPUnit\Framework\Assert;

class OdbcExportAdapterTest extends AbstractExportAdapterTest
{
    use OdbcCreateConnectionTrait;

    protected function createExportAdapter(
        array $state = [],
        ?string $host = null,
        ?int $port = null,
        ?QueryFactory $queryFactory = null,
    ): OdbcExportAdapter {
        $connection = $this->createOdbcConnection($host, $port);
        $queryFactory = $queryFactory ?? new DefaultQueryFactory($state);
        return new OdbcExportAdapter(
            $this->logger,
            $connection,
            $queryFactory,
            $this->createResultWriter($state),
            $this->temp->getTmpFolder(),
            $state,
        );
    }

    public function testGetName(): void
    {
        Assert::assertSame('ODBC', $this->createExportAdapter()->getName());
    }
}
