<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Extractor\MySQL;
use Keboola\DbExtractorLogger\Logger;

class QueryGenerationTest extends AbstractMySQLTest
{
    /** @var array */
    private $config;

    public function setUp(): void
    {
        $this->dataDir = __DIR__ . '/../../data';
        $this->config = $this->getConfigRow(self::DRIVER);
    }

    /**
     * @dataProvider simpleTableColumnsDataProvider
     */
    public function testGetSimplifiedPdoQuery(array $params, array $state, string $expected): void
    {
        $extractor = new MySQL($this->config['parameters'], $state, new Logger('mssql-extractor-test'));

        if (isset($params['incrementalFetchingColumn']) && $params['incrementalFetchingColumn'] !== '') {
            $extractor->validateIncrementalFetching(
                $params['table'],
                $params['incrementalFetchingColumn'],
                isset($params['incrementalFetchingLimit']) ? $params['incrementalFetchingLimit'] : null
            );
        }
        $query = $extractor->simpleQuery($params['table'], $params['columns']);
        $this->assertEquals($expected, $query);
    }

    public function simpleTableColumnsDataProvider(): array
    {
        return [
            // simple table select with all columns
            [
                [
                    'table' => [
                        'tableName' => 'test',
                        'schema' => 'testSchema',
                    ],
                    'columns' => [],
                ],
                [],
                'SELECT * FROM `testSchema`.`test`',
            ],
            // simple table select with all columns (columns as null)
            [
                [
                    'table' => [
                        'tableName' => 'test',
                        'schema' => 'testSchema',
                    ],
                    'columns' => [],
                ],
                [],
                'SELECT * FROM `testSchema`.`test`',
            ],
            // simple table with 2 columns selected
            [
                [
                    'table' => [
                        'tableName' => 'test',
                        'schema' => 'testSchema',
                    ],
                    'columns' => ['col1', 'col2'],
                ],
                [],
                'SELECT `col1`, `col2` FROM `testSchema`.`test`',
            ],
            // test simplePDO query with limit and timestamp column but no state
            [
                [
                    'table' => [
                        'tableName' => 'auto_increment_timestamp',
                        'schema' => 'test',
                    ],
                    'columns' => [],
                    'incrementalFetchingLimit' => 10,
                    'incrementalFetchingColumn' => 'timestamp',
                ],
                [],
                'SELECT * FROM `test`.`auto_increment_timestamp` ORDER BY `timestamp` LIMIT 10',
            ],
            // test simplePDO query with limit and idp column and previos state
            [
                [
                    'table' => [
                        'tableName' => 'auto_increment_timestamp',
                        'schema' => 'test',
                    ],
                    'columns' => [],
                    'incrementalFetchingLimit' => 10,
                    'incrementalFetchingColumn' => '_weird-I-d',
                ],
                [
                    'lastFetchedRow' => 4,
                ],
                'SELECT * FROM `test`.`auto_increment_timestamp` WHERE `_weird-I-d` >= \'4\' ORDER BY `_weird-I-d` LIMIT 10',
            ],
            // test simplePDO query timestamp column but no state and no limit
            [
                [
                    'table' => [
                        'tableName' => 'auto_increment_timestamp',
                        'schema' => 'test',
                    ],
                    'columns' => [],
                    'incrementalFetchingLimit' => null,
                    'incrementalFetchingColumn' => 'timestamp',
                ],
                [],
                'SELECT * FROM `test`.`auto_increment_timestamp` ORDER BY `timestamp`',
            ],
            // test simplePDO query id column and previos state and no limit
            [
                [
                    'table' => [
                        'tableName' => 'auto_increment_timestamp',
                        'schema' => 'test',
                    ],
                    'columns' => [],
                    'incrementalFetchingLimit' => 0,
                    'incrementalFetchingColumn' => '_weird-I-d',
                ],
                [
                    'lastFetchedRow' => 4,
                ],
                'SELECT * FROM `test`.`auto_increment_timestamp` WHERE `_weird-I-d` >= \'4\' ORDER BY `_weird-I-d`',
            ],
        ];
    }
}
