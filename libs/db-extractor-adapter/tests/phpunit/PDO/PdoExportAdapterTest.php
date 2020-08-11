<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\Tests\PDO;

use Keboola\DbExtractor\Adapter\PDO\PdoExportAdapter;
use Keboola\DbExtractor\Adapter\Tests\AbstractExportAdapterTest;
use Keboola\DbExtractor\Adapter\Query\DefaultSimpleQueryFactory;
use Keboola\DbExtractor\Adapter\Query\SimpleQueryFactory;
use Keboola\DbExtractor\Adapter\Tests\Traits\PdoCreateConnectionTrait;
use PHPUnit\Framework\Assert;

class PdoExportAdapterTest extends AbstractExportAdapterTest
{
    use PdoCreateConnectionTrait;

    protected function createExportAdapter(
        array $state = [],
        ?string $host = null,
        ?int $port = null,
        ?SimpleQueryFactory $queryFactory = null
    ): PdoExportAdapter {
        $connection = $this->createPdoConnection($host, $port);
        $queryFactory = $queryFactory ?? new DefaultSimpleQueryFactory($state);
        return new PdoExportAdapter(
            $this->logger,
            $connection,
            $queryFactory,
            $this->temp->getTmpFolder(),
            $state
        );
    }

    public function testGetName(): void
    {
        Assert::assertSame('PDO', $this->createExportAdapter()->getName());
    }
}
