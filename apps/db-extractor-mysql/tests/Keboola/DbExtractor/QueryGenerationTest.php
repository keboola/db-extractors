<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Extractor\MySQL;
use Keboola\Component\Logger;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;

class QueryGenerationTest extends AbstractMySQLTest
{
    public function setUp(): void
    {
        parent::setUp();
        $this->dataDir = __DIR__ . '/../../data';
    }
    /**
     * @dataProvider simpleTableColumnsDataProvider
     */
    public function testGetSimplifiedPdoQuery(array $params, array $state, string $expected): void
    {
        $this->createAutoIncrementAndTimestampTable();
        $config = $this->getConfigRow(self::DRIVER);
        $config['parameters'] = array_merge($config['parameters'], $params);
        $config['parameters']['query'] = empty($config['parameters']['table']) ?
            $config['parameters']['query'] : null;

        $exportConfig = ExportConfig::fromArray($config['parameters']);
        $extractor = new MySQL($config['parameters'], $state, new Logger());
        if ($exportConfig->isIncrementalFetching()) {
            $extractor->validateIncrementalFetching($exportConfig);
        }
        $query = $extractor->simpleQuery($exportConfig);
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
                    'incremental' => true,
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
                    'incremental' => true,
                    'incrementalFetchingLimit' => 10,
                    'incrementalFetchingColumn' => '_weird-I-d',
                ],
                [
                    'lastFetchedRow' => 4,
                ],
                "SELECT * FROM `test`.`auto_increment_timestamp` WHERE `_weird-I-d` >= '4' ORDER BY `_weird-I-d` LIMIT 10",
            ],
            // test simplePDO query timestamp column but no state and no limit
            [
                [
                    'table' => [
                        'tableName' => 'auto_increment_timestamp',
                        'schema' => 'test',
                    ],
                    'columns' => [],
                    'incremental' => true,
                    'incrementalFetchingLimit' => null,
                    'incrementalFetchingColumn' => 'timestamp',
                ],
                [],
                'SELECT * FROM `test`.`auto_increment_timestamp` ORDER BY `timestamp`',
            ],
            // test simplePDO query id column and previous state and no limit
            [
                [
                    'table' => [
                        'tableName' => 'auto_increment_timestamp',
                        'schema' => 'test',
                    ],
                    'columns' => [],
                    'incremental' => true,
                    'incrementalFetchingLimit' => null,
                    'incrementalFetchingColumn' => '_weird-I-d',
                ],
                [
                    'lastFetchedRow' => 4,
                ],
                "SELECT * FROM `test`.`auto_increment_timestamp` WHERE `_weird-I-d` >= '4' ORDER BY `_weird-I-d`",
            ],
        ];
    }
}
