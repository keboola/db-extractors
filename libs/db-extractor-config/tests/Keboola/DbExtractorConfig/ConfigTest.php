<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Tests;

use Keboola\DbExtractorConfig\Config;
use Keboola\DbExtractorConfig\Configuration\ActionConfigRowDefinition;
use Keboola\DbExtractorConfig\Configuration\ConfigDefinition;
use Keboola\DbExtractorConfig\Configuration\ConfigRowDefinition;
use Keboola\DbExtractorConfig\Exception\UserException as ConfigUserException;
use Keboola\DbExtractorConfig\Test\AbstractConfigTest;

class ConfigTest extends AbstractConfigTest
{
    public const DRIVER = 'config';

    public function testConfig(): void
    {
        $configurationArray = [
            'parameters' => [
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'db' => [
                    'host' => 'mysql',
                    'user' => 'root',
                    '#password' => 'rootpassword',
                    'database' => 'test',
                    'port' => 3306,
                ],
                'tables' => [
                    [
                        'id' => 1,
                        'name' => 'sales',
                        'query' => 'SELECT * FROM sales',
                        'outputTable' => 'in.c-main.sales',
                        'incremental' => false,
                        'primaryKey' => [],
                        'enabled' => true,
                        'columns' => [],
                    ],
                    [
                        'id' => 2,
                        'name' => 'escaping',
                        'query' => 'SELECT * FROM escaping',
                        'outputTable' => 'in.c-main.escaping',
                        'incremental' => false,
                        'primaryKey' => [
                            0 => 'orderId',
                        ],
                        'enabled' => true,
                        'columns' => [],
                    ],
                    [
                        'id' => 3,
                        'enabled' => true,
                        'name' => 'tableColumns',
                        'outputTable' => 'in.c-main.tableColumns',
                        'incremental' => false,
                        'primaryKey' => [],
                        'table' => [
                            'schema' => 'test',
                            'tableName' => 'sales',
                        ],
                        'columns' => [
                            0 => 'usergender',
                            1 => 'usercity',
                            2 => 'usersentiment',
                            3 => 'zipcode',
                        ],
                    ],
                ],
            ],
        ];

        $config = new Config($configurationArray, new ConfigDefinition());

        $this->assertEquals($configurationArray, $config->getData());
    }

    public function testConfigRow(): void
    {
        $configurationArray = [
            'parameters' => [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'incremental' => true,
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'table' => [
                    'tableName' => 'auto_increment_timestamp',
                    'schema' => 'test',
                ],
                'name' => 'auto-increment-timestamp',
                'incrementalFetchingColumn' => '_weird-I-d',
                'primaryKey' => [],
                'columns' => [],
                'enabled' => true
            ],
        ];

        $config = new Config($configurationArray, new ConfigRowDefinition());

        $this->assertEquals($configurationArray, $config->getData());
    }

    public function testConfigActionRow(): void
    {
        $configurationArray = [
            'parameters' => [
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'db' => [
                    'host' => 'mysql',
                    'user' => 'root',
                    '#password' => 'rootpassword',
                    'database' => 'test',
                    'port' => 3306,
                ],
            ],
        ];

        $config = new Config($configurationArray, new ActionConfigRowDefinition());

        $this->assertEquals($configurationArray, $config->getData());
    }

    public function testInvalidConfigQueryIncremental(): void
    {
        $configurationArray = [
            'parameters' => [
                'outputTable' => 'fake.output',
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'query' => 'select 1 from test',
                'incrementalFetchingColumn' => 'test',
            ],
        ];

        $exceptionMessage = 'Incremental fetching is not supported for advanced queries.';
        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage($exceptionMessage);

        new Config($configurationArray, new ConfigRowDefinition());
    }

    public function testInvalidConfigTableOrQuery(): void
    {
        $configurationArray = [
            'parameters' => [
                'outputTable' => 'fake.output',
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
            ],
        ];

        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage('One of table or query is required');

        new Config($configurationArray, new ConfigRowDefinition());
    }

    public function testInvalidConfigsNeitherTableNorQueryWithNoName(): void
    {
        $configurationArray = [
            'parameters' => [
                'outputTable' => 'fake.output',
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'table' => [
                    'schema' => 'test',
                ],
            ],
        ];

        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage('The child node "tableName" at path "root.parameters.table" must be configured.');

        new Config($configurationArray, new ConfigRowDefinition());
    }

    public function testInvalidConfigsInvalidTableWithNoName(): void
    {
        $configurationArray = [
            'parameters' => [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'table' => [
                    'tableName' => 'auto_increment_timestamp',
                ],
            ],
        ];

        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage('The child node "schema" at path "root.parameters.table" must be configured.');

        new Config($configurationArray, new ConfigRowDefinition());
    }

    public function testInvalidConfigsBothTableAndQuery(): void
    {
        $configurationArray = [
            'parameters' => [
                'outputTable' => 'fake.output',
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'table' => [
                    'tableName' => 'test',
                    'schema' => 'test',
                ],
                'query' => 'select 1 from test',
            ],
        ];

        $exceptionMessage = 'Both table and query cannot be set together.';

        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage($exceptionMessage);

        new Config($configurationArray, new ConfigRowDefinition());
    }

    public function testInvalidConfigsBothIncrFetchAndQueryWithNoName(): void
    {
        $configurationArray = [
            'parameters' => [
                'outputTable' => 'fake.output',
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'incrementalFetchingColumn' => 'abc',
                'query' => 'select 1 limit 0',
            ],
        ];

        $exceptionMessage = 'Incremental fetching is not supported for advanced queries.';

        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage($exceptionMessage);

        new Config($configurationArray, new ConfigRowDefinition());
    }
}
