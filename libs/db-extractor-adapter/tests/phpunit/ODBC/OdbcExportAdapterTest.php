<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\Tests\ODBC;

use PHPUnit\Framework\Assert;
use Keboola\DbExtractor\Adapter\Tests\AbstractExportAdapterTest;
use Keboola\DbExtractor\Adapter\ODBC\OdbcExportAdapter;
use Keboola\DbExtractor\Adapter\Query\DefaultSimpleQueryFactory;
use Keboola\DbExtractor\Adapter\Query\SimpleQueryFactory;
use Keboola\DbExtractor\Adapter\Tests\Traits\OdbcCreateConnectionTrait;

class OdbcExportAdapterTest extends AbstractExportAdapterTest
{
    use OdbcCreateConnectionTrait;

    protected function createExportAdapter(
        array $state = [],
        ?string $host = null,
        ?int $port = null,
        ?SimpleQueryFactory $queryFactory = null
    ): OdbcExportAdapter {
        $connection = $this->createOdbcConnection($host, $port);
        $queryFactory = $queryFactory ?? new DefaultSimpleQueryFactory($state);
        return new OdbcExportAdapter(
            $this->logger,
            $connection,
            $queryFactory,
            $this->temp->getTmpFolder(),
            $state
        );
    }

    public function testGetName(): void
    {
        Assert::assertSame('ODBC', $this->createExportAdapter()->getName());
    }
}
