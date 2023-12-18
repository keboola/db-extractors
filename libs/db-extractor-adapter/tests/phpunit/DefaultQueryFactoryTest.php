<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\Tests;

use Keboola\DbExtractor\Adapter\Query\DefaultQueryFactory;
use Keboola\DbExtractor\Adapter\Tests\Traits\PdoCreateConnectionTrait;
use PHPUnit\Framework\Assert;

class DefaultQueryFactoryTest extends BaseTest
{
    use PdoCreateConnectionTrait;

    /**
     * @dataProvider getQueryFactoryInputs
     */
    public function testQueryFactory(string $expectedQuery, array $exportConfig, array $state = []): void
    {
        $queryFactory = new DefaultQueryFactory($state);
        $query = $queryFactory->create($this->createExportConfig($exportConfig), $this->createPdoConnection());
        Assert::assertSame($expectedQuery, $query);
    }

    public function getQueryFactoryInputs(): array
    {
        return [
            'minimal' => [
                'SELECT * FROM `bar`.`foo`',
                [
                    'table' => ['tableName' => 'foo', 'schema' => 'bar'],
                ],
            ],
            'columns' => [
                'SELECT `col1`, `col2` FROM `bar`.`foo`',
                [
                    'table' => ['tableName' => 'foo', 'schema' => 'bar'],
                    'columns' => ['col1', 'col2'],
                ],
            ],
            'incrementalFetchingNoState' => [
                'SELECT * FROM `bar`.`foo` ORDER BY `col2`',
                [
                    'table' => ['tableName' => 'foo', 'schema' => 'bar'],
                    'incrementalFetchingColumn' => 'col2',
                ],
            ],
            'incrementalFetchingNoStateColumns' => [
                'SELECT `col1`, `col2` FROM `bar`.`foo` ORDER BY `col2`',
                [
                    'table' => ['tableName' => 'foo', 'schema' => 'bar'],
                    'columns' => ['col1', 'col2'],
                    'incrementalFetchingColumn' => 'col2',
                ],
            ],
            'incrementalFetchingState' => [
                'SELECT * FROM `bar`.`foo` WHERE `col2` >= \'123\' ORDER BY `col2`',
                [
                    'table' => ['tableName' => 'foo', 'schema' => 'bar'],
                    'incrementalFetchingColumn' => 'col2',
                ],
                [
                    'lastFetchedRow' => '123',
                ],
            ],
            'incrementalFetchingStateColumns' => [
                'SELECT `col1`, `col2` FROM `bar`.`foo` WHERE `col2` >= \'123\' ORDER BY `col2`',
                [
                    'table' => ['tableName' => 'foo', 'schema' => 'bar'],
                    'columns' => ['col1', 'col2'],
                    'incrementalFetchingColumn' => 'col2',
                ],
                [
                    'lastFetchedRow' => '123',
                ],
            ],
            'incrementalFetchingStateLimit' => [
                'SELECT * FROM `bar`.`foo` WHERE `col2` >= \'123\' ORDER BY `col2` LIMIT 321',
                [
                    'table' => ['tableName' => 'foo', 'schema' => 'bar'],
                    'incrementalFetchingColumn' => 'col2',
                    'incrementalFetchingLimit' => 321,
                ],
                [
                    'lastFetchedRow' => '123',
                ],
            ],
            'incrementalFetchingStateLimitColumns' => [
                'SELECT `col1`, `col2` FROM `bar`.`foo` WHERE `col2` >= \'123\' ORDER BY `col2` LIMIT 321',
                [
                    'table' => ['tableName' => 'foo', 'schema' => 'bar'],
                    'columns' => ['col1', 'col2'],
                    'incrementalFetchingColumn' => 'col2',
                    'incrementalFetchingLimit' => 321,
                ],
                [
                    'lastFetchedRow' => '123',
                ],
            ],
        ];
    }
}
