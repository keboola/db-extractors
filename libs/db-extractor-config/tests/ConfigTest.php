<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Tests;

use Keboola\DbExtractorConfig\Config;
use Keboola\DbExtractorConfig\Configuration\ActionConfigRowDefinition;
use Keboola\DbExtractorConfig\Configuration\ConfigDefinition;
use Keboola\DbExtractorConfig\Configuration\ConfigRowDefinition;
use Keboola\DbExtractorConfig\Configuration\GetTablesListFilterDefinition;
use Keboola\DbExtractorConfig\Exception\UserException as ConfigUserException;
use Keboola\DbExtractorConfig\Test\AbstractConfigTest;
use PHPUnit\Framework\Assert;

class ConfigTest extends AbstractConfigTest
{
    public const DRIVER = 'config';

    public function testConfig(): void
    {
        $configurationArray = [
            'parameters' => [
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'db' => $this->getDbConfigurationArray(),
                'tables' => [
                    [
                        'id' => 1,
                        'name' => 'sales',
                        'query' => 'SELECT * FROM sales',
                        'outputTable' => 'in.c-main.sales',
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
                        'retries' => 20,
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
                        'retries' => 30,
                    ],
                ],
            ],
        ];
        $config = new Config($configurationArray, new ConfigDefinition());

        $expected = [
            'parameters' => [
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'db' => $this->getExpectedDbConfigArray(),
                'tables' => [
                    [
                        'id' => 1,
                        'name' => 'sales',
                        'query' => 'SELECT * FROM sales',
                        'outputTable' => 'in.c-main.sales',
                        // Default values:
                        'incremental' => false,
                        'primaryKey' => [],
                        'enabled' => true,
                        'columns' => [],
                        'retries' => 5,
                        'incrementalFetchingColumn' => null,
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
                        'retries' => 20,
                        'incrementalFetchingColumn' => null,
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
                        'retries' => 30,
                        'query' => null,
                        'incrementalFetchingColumn' => null,
                    ],
                ],
            ],
        ];
        $this->assertEquals($expected, $config->getData());
    }

    public function testConfigRow(): void
    {
        $configurationArray = [
            'parameters' => [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'incremental' => true,
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'db' => $this->getDbConfigurationArray(),
                'table' => [
                    'tableName' => 'auto_increment_timestamp',
                    'schema' => 'test',
                ],
                'name' => 'auto-increment-timestamp',
                'incrementalFetchingColumn' => '_weird-I-d',
            ],
        ];

        $config = new Config($configurationArray, new ConfigRowDefinition());

        $expected = [
            'parameters' => [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'incremental' => true,
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'db' => $this->getExpectedDbConfigArray(),
                'table' => [
                    'tableName' => 'auto_increment_timestamp',
                    'schema' => 'test',
                ],
                'name' => 'auto-increment-timestamp',
                'incrementalFetchingColumn' => '_weird-I-d',
                // Default values:
                'retries' => 5,
                'columns' => [],
                'enabled' => true,
                'primaryKey' => [],
                'query' => null,
            ],
        ];
        $this->assertEquals($expected, $config->getData());
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
                    'initQueries' => [],
                ],
            ],
        ];

        $config = new Config($configurationArray, new ActionConfigRowDefinition());
        $this->assertEquals($configurationArray, $config->getData());
    }

    public function testConfigWithSshTunnel(): void
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
                    'initQueries' => [],
                    'ssh' => [
                        'user' => 'root',
                        'sshHost' => 'sshproxy',
                        'sshPort' => '22',
                        'localPort' => '33306',
                        'keys' => ['public' => 'anyKey'],
                        'maxRetries' => 10,
                        'compression' => false,
                    ],
                ],
            ],
        ];

        $config = new Config($configurationArray, new ActionConfigRowDefinition());
        $this->assertEquals($configurationArray, $config->getData());
    }

    public function testConfigWithSshTunnelDisabled(): void
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
                    'initQueries' => [],
                    'ssh' => [
                        'enabled' => false,
                        'sshHost' => 'sshproxy',
                        'sshPort' => '22',
                        'localPort' => '33306',
                        'keys' => ['public' => 'anyKey'],
                        'maxRetries' => 10,
                        'compression' => false,
                    ],
                ],
            ],
        ];

        $config = new Config($configurationArray, new ActionConfigRowDefinition());
        $this->assertEquals($configurationArray, $config->getData());
    }

    public function testConfigWithSshTunnelEnabled(): void
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
                    'initQueries' => [],
                    'ssh' => [
                        'enabled' => true,
                        'user' => 'root',
                        'sshHost' => 'sshproxy',
                        'sshPort' => '22',
                        'localPort' => '33306',
                        'keys' => ['public' => 'anyKey', '#private' => 'anyKey'],
                        'maxRetries' => 10,
                        'compression' => false,
                    ],
                ],
            ],
        ];

        $config = new Config($configurationArray, new ActionConfigRowDefinition());
        $this->assertEquals($configurationArray, $config->getData());
    }

    public function testConfigWithSshTunnelEnabledMissingPrivateKey(): void
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
                    'initQueries' => [],
                    'ssh' => [
                        'enabled' => true,
                        'user' => 'root',
                        'sshHost' => 'sshproxy',
                        'sshPort' => '22',
                        'localPort' => '33306',
                        'keys' => ['public' => 'anyKey'],
                        'maxRetries' => 10,
                        'compression' => false,
                    ],
                ],
            ],
        ];

        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage('The child config "#private" under "root.parameters.db.ssh.keys" ' .
            'must be configured.');

        new Config($configurationArray, new ActionConfigRowDefinition());
    }

    public function testUnencryptedSslKey(): void
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
                    'ssl' => [
                        'key' => 'testKey',
                        'ca' => 'testCa',
                        'cert' => 'testCert',
                        'cipher' => 'testCipher',
                        'verifyServerCert' => false,
                    ],
                ],
            ],
        ];

        $config = new Config($configurationArray, new ActionConfigRowDefinition());
        $this->assertEquals('testKey', $config->getData()['parameters']['db']['ssl']['#key']);
    }

    public function testSslOnlyCa(): void
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
                    'ssl' => [
                        'enabled' => true,
                        'ca' => 'abs',
                    ],
                ],
            ],
        ];

        $this->expectNotToPerformAssertions();
        new Config($configurationArray, new ActionConfigRowDefinition());
    }

    public function testMissingSslKey(): void
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
                    'ssl' => [
                        'enabled' => true,
                        'cert' => 'abs',
                    ],
                ],
            ],
        ];

        $exceptionMessage =
            'Invalid configuration for path "root.parameters.db.ssl": ' .
            'Both "#key" and "cert" must be specified.';
        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage($exceptionMessage);

        new Config($configurationArray, new ConfigRowDefinition());
    }

    public function testMissingSslCert(): void
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
                    'ssl' => [
                        'enabled' => true,
                        '#key' => 'abs',
                    ],
                ],
            ],
        ];

        $exceptionMessage =
            'Invalid configuration for path "root.parameters.db.ssl": ' .
            'Both "#key" and "cert" must be specified.';
        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage($exceptionMessage);

        new Config($configurationArray, new ConfigRowDefinition());
    }

    public function testInvalidConfigQueryIncremental(): void
    {
        $configurationArray = [
            'parameters' => [
                'outputTable' => 'fake.output',
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'db' => $this->getDbConfigurationArray(),
                'query' => 'select 1 from test',
                'incrementalFetchingColumn' => 'test',
            ],
        ];

        $exceptionMessage =
            'The "incrementalFetchingColumn" is configured, ' .
            'but incremental fetching is not supported for custom query.';
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
                'db' => $this->getDbConfigurationArray(),
            ],
        ];

        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage('Table or query must be configured.');

        new Config($configurationArray, new ConfigRowDefinition());
    }

    public function testInvalidConfigsNeitherTableNorQueryWithNoName(): void
    {
        $configurationArray = [
            'parameters' => [
                'outputTable' => 'fake.output',
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'db' => $this->getDbConfigurationArray(),
                'table' => [
                    'schema' => 'test',
                ],
            ],
        ];

        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage('The child config "tableName" under "root.parameters.table" must be configured.');

        new Config($configurationArray, new ConfigRowDefinition());
    }

    public function testInvalidConfigsInvalidTableWithNoName(): void
    {
        $configurationArray = [
            'parameters' => [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'db' => $this->getDbConfigurationArray(),
                'table' => [
                    'tableName' => 'auto_increment_timestamp',
                ],
            ],
        ];

        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage('The child config "schema" under "root.parameters.table" must be configured.');

        new Config($configurationArray, new ConfigRowDefinition());
    }

    public function testInvalidConfigsBothTableAndQuery(): void
    {
        $configurationArray = [
            'parameters' => [
                'outputTable' => 'fake.output',
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'db' => $this->getDbConfigurationArray(),
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
                'db' => $this->getDbConfigurationArray(),
                'incrementalFetchingColumn' => 'abc',
                'query' => 'select 1 limit 0',
            ],
        ];

        $exceptionMessage =
            'The "incrementalFetchingColumn" is configured, ' .
            'but incremental fetching is not supported for custom query.';

        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage($exceptionMessage);

        new Config($configurationArray, new ConfigRowDefinition());
    }

    public function testTestConfigWithExtraKeysConfigDefinition(): void
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
                    'initQueries' => [],
                ],
                'tables' => [],
                'advancedMode' => true,
            ],
        ];

        $config = new Config($configurationArray, new ConfigDefinition());
        $this->assertEquals($configurationArray, $config->getData());
    }

    public function testTestConfigWithExtraKeysConfigRowDefinition(): void
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
                    'initQueries' => [],
                ],
                'query' => 'SELECT 1 FROM test',
                'outputTable' => 'testOutput',
                'columns' => [],
                'incrementalFetchingColumn' => null,
                'incremental' => false,
                'enabled' => true,
                'primaryKey' => [],
                'advancedMode' => true,
                'retries' => 10,
            ],
        ];

        $config = new Config($configurationArray, new ConfigRowDefinition());
        $this->assertEquals($configurationArray, $config->getData());
    }

    public function testTestConfigWithExtraKeysActionConfigRowDefinition(): void
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
                    'initQueries' => [],
                ],
                'advancedMode' => true,
            ],
        ];

        $config = new Config($configurationArray, new ActionConfigRowDefinition());
        $this->assertEquals($configurationArray, $config->getData());
    }

    public function testQueryCannotBeEmpty(): void
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
                'query' => '',
                'outputTable' => 'testOutput',
            ],
        ];

        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage('"root.parameters.query" cannot contain an empty value, but got "".');
        new Config($configurationArray, new ConfigRowDefinition());
    }

    public function testTableSchemaCannotBeEmpty(): void
    {
        $configurationArray = [
            'parameters' => [
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'db' => $this->getDbConfigurationArray(),
                'table' => [
                    'tableName' => 'auto_increment_timestamp',
                    'schema' => '',
                ],
            ],
        ];

        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage('"root.parameters.table.schema" cannot contain an empty value, but got "".');
        new Config($configurationArray, new ConfigRowDefinition());
    }

    public function testTableNameCannotBeEmpty(): void
    {
        $configurationArray = [
            'parameters' => [
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'db' => $this->getDbConfigurationArray(),
                'table' => [
                    'tableName' => '',
                    'schema' => 'schema',
                ],
            ],
        ];

        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage('"root.parameters.table.tableName" cannot contain an empty value, but got "".');
        new Config($configurationArray, new ConfigRowDefinition());
    }

    public function testIncrementalLoadingWithNoIncrementalFetchingColumn(): void
    {
        $configurationArray = [
            'parameters' => [
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'db' => $this->getDbConfigurationArray(),
                'table' => [
                    'tableName' => 'name',
                    'schema' => 'schema',
                ],
                'incremental' => true,
            ],
        ];

        $config = new Config($configurationArray, new ConfigRowDefinition());
        Assert::assertSame([
            'parameters' => [
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'db' => $this->getExpectedDbConfigArray(),
                'table' => [
                    'tableName' => 'name',
                    'schema' => 'schema',
                ],
                'incremental' => true,
                'query' => null,
                'columns' => [],
                'incrementalFetchingColumn' => null,
                'enabled' => true,
                'primaryKey' => [],
                'retries' => 5,
            ],
        ], $config->getData());
    }

    public function testIncrementalFetchingColumnAndNoIncrementalLoading(): void
    {
        $configurationArray = [
            'parameters' => [
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'db' => $this->getDbConfigurationArray(),
                'table' => [
                    'tableName' => 'name',
                    'schema' => 'schema',
                ],
                'incremental' => false,
                'incrementalFetchingColumn' => 'name',
            ],
        ];

        $config = new Config($configurationArray, new ConfigRowDefinition());
        Assert::assertSame([
            'parameters' => [
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'db' => $this->getExpectedDbConfigArray(),
                'table' => [
                    'tableName' => 'name',
                    'schema' => 'schema',
                ],
                'incremental' => false,
                'incrementalFetchingColumn' => 'name',
                'query' => null,
                'columns' => [],
                'enabled' => true,
                'primaryKey' => [],
                'retries' => 5,
            ],
        ], $config->getData());
    }

    public function testIncrementalFetchingLimitButNoColumn(): void
    {
        $configurationArray = [
            'parameters' => [
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'db' => $this->getDbConfigurationArray(),
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'table' => [
                    'tableName' => 'name',
                    'schema' => 'schema',
                ],
                'incremental' => false,
                'incrementalFetchingLimit' => 100,
            ],
        ];

        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage(
            'The "incrementalFetchingLimit" is configured, but "incrementalFetchingColumn" is missing.'
        );
        new Config($configurationArray, new ConfigRowDefinition());
    }

    public function testEmptyColumnName(): void
    {
        $configurationArray = [
            'parameters' => [
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'db' => $this->getDbConfigurationArray(),
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'table' => [
                    'tableName' => 'name',
                    'schema' => 'schema',
                ],
                'columns' => ['abc', ''],
            ],
        ];

        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage(
            'The path "root.parameters.columns.1" cannot contain an empty value, but got "".'
        );
        new Config($configurationArray, new ConfigRowDefinition());
    }

    public function testEmptyNameInPK(): void
    {
        $configurationArray = [
            'parameters' => [
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'db' => $this->getDbConfigurationArray(),
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'table' => [
                    'tableName' => 'name',
                    'schema' => 'schema',
                ],
                'primaryKey' => ['abc', ''],
            ],
        ];

        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage(
            'The path "root.parameters.primaryKey.1" cannot contain an empty value, but got "".'
        );
        new Config($configurationArray, new ConfigRowDefinition());
    }

    public function testIncrementalFetchingLimit(): void
    {
        $configurationArray = [
            'parameters' => [
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'db' => $this->getDbConfigurationArray(),
                'table' => [
                    'tableName' => 'table',
                    'schema' => 'schema',
                ],
                'outputTable' => 'output-table',
                'retries' => 12,
                'columns' => [],
                'primaryKey' => [],
                'incremental' => true,
                'incrementalFetchingColumn' => 'col123',
                'incrementalFetchingLimit' => 456, // <<<<<<<<<<
            ],
        ];

        // Normalized
        $expected = $configurationArray;
        $expected['parameters']['db'] = $this->getExpectedDbConfigArray();
        $expected['parameters']['query'] = null;
        $expected['parameters']['enabled'] = true;

        $config = new Config($configurationArray, new ConfigRowDefinition());
        $this->assertEquals($expected, $config->getData());
    }

    public function testIncrementalFetchingLimitZero(): void
    {
        $configurationArray = [
            'parameters' => [
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'db' => $this->getDbConfigurationArray(),
                'table' => [
                    'tableName' => 'table',
                    'schema' => 'schema',
                ],
                'outputTable' => 'output-table',
                'retries' => 12,
                'columns' => [],
                'primaryKey' => [],
                'incremental' => true,
                'incrementalFetchingColumn' => 'col123',
                'incrementalFetchingLimit' => 0, // <<<<<<<<<<
            ],
        ];

        // Normalized
        $expected = $configurationArray;
        $expected['parameters']['db'] = $this->getExpectedDbConfigArray();
        $expected['parameters']['incrementalFetchingLimit'] = null;
        $expected['parameters']['query'] = null;
        $expected['parameters']['enabled'] = true;

        $config = new Config($configurationArray, new ConfigRowDefinition());
        $this->assertEquals($expected, $config->getData());
    }

    public function testGetTablesSimple(): void
    {
        $configurationArray = [
            'action' => 'getTables',
            'parameters' => [
                'db' => $this->getDbConfigurationArray(),
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
            ],
        ];

        new Config($configurationArray, new GetTablesListFilterDefinition());
        $this->expectNotToPerformAssertions(); // no error expected
    }

    public function testGetTablesListFilter(): void
    {
        $configurationArray = [
            'action' => 'getTables',
            'parameters' => [
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'db' => $this->getDbConfigurationArray(),
                'tableListFilter' => [
                    'listColumns' => true,
                    'tablesToList' => [
                        [
                            'tableName' => 'table1',
                            'schema' => 'default',
                        ],
                        [
                            'tableName' => 'table2',
                            'schema' => 'default',
                        ],
                    ],
                ],
            ],
        ];

        new Config($configurationArray, new GetTablesListFilterDefinition());
        $this->expectNotToPerformAssertions(); // no error expected
    }

    public function testGetTablesListFilterMissingTables(): void
    {
        $configurationArray = [
            'action' => 'getTables',
            'parameters' => [
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'db' => $this->getDbConfigurationArray(),
                'tableListFilter' => [
                    'listColumns' => true,
                    'tablesToList' => [],
                ],
            ],
        ];

        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage('The path "root.parameters.tableListFilter.tablesToList" should have at least ' .
            '1 element(s) defined.');
        new Config($configurationArray, new GetTablesListFilterDefinition());
    }

    public function testGetTablesListFilterMissingTableName(): void
    {
        $configurationArray = [
            'action' => 'getTables',
            'parameters' => [
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'db' => $this->getDbConfigurationArray(),
                'tableListFilter' => [
                    'listColumns' => true,
                    'tablesToList' => [
                        [
                            'wrongIndex' => 'table1',
                        ],
                    ],
                ],
            ],
        ];

        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage('Unrecognized option "wrongIndex" under "root.parameters.tableListFilter.' .
            'tablesToList.0". Available options are "schema", "tableName".');
        new Config($configurationArray, new GetTablesListFilterDefinition());
    }

    public function testGetTablesListFilterEmptyTablesToListArray(): void
    {
        $configurationArray = [
            'action' => 'getTables',
            'parameters' => [
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'db' => $this->getDbConfigurationArray(),
                'tableListFilter' => [
                    'listColumns' => true,
                    'tablesToList' => [
                        [],
                    ],
                ],
            ],
        ];

        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage('The child config "tableName" under ' .
            '"root.parameters.tableListFilter.tablesToList.0" must be configured.');
        new Config($configurationArray, new GetTablesListFilterDefinition());
    }

    public function testGetTablesListFilterEmptyTableName(): void
    {
        $configurationArray = [
            'action' => 'getTables',
            'parameters' => [
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'db' => $this->getDbConfigurationArray(),
                'tableListFilter' => [
                    'listColumns' => true,
                    'tablesToList' => [
                        ['tableName' => ''],
                    ],
                ],
            ],
        ];

        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage('The path "root.parameters.tableListFilter.tablesToList.0.tableName" cannot ' .
            'contain an empty value, but got "".');
        new Config($configurationArray, new GetTablesListFilterDefinition());
    }

    public function testGetTablesListFilterMissingSchema(): void
    {
        $configurationArray = [
            'action' => 'getTables',
            'parameters' => [
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'db' => $this->getDbConfigurationArray(),
                'tableListFilter' => [
                    'listColumns' => true,
                    'tablesToList' => [
                        [
                            'tableName' => 'table1',
                        ],
                    ],
                ],
            ],
        ];

        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage('The child config "schema" under ' .
            '"root.parameters.tableListFilter.tablesToList.0" must be configured.');
        new Config($configurationArray, new GetTablesListFilterDefinition());
    }

    public function testIncrementalFetchingLimitNull(): void
    {
        $configurationArray = [
            'parameters' => [
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'db' => $this->getDbConfigurationArray(),
                'table' => [
                    'tableName' => 'table',
                    'schema' => 'schema',
                ],
                'outputTable' => 'output-table',
                'retries' => 12,
                'columns' => [],
                'primaryKey' => [],
                'incremental' => false,
                'incrementalFetchingLimit' => null, // <<<<<<<<<<
            ],
        ];

        $config = new Config($configurationArray, new ConfigRowDefinition());
        Assert::assertSame(null, $config->getData()['incrementalFetchingLimit']);
    }

    public function testIncrementalFetchingColumnNull(): void
    {
        $configurationArray = [
            'parameters' => [
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
                'db' => $this->getDbConfigurationArray(),
                'table' => [
                    'tableName' => 'table',
                    'schema' => 'schema',
                ],
                'outputTable' => 'output-table',
                'retries' => 12,
                'columns' => [],
                'primaryKey' => [],
                'incremental' => false,
                'incrementalFetchingColumn' => null, // <<<<<<<<<<
            ],
        ];

        $config = new Config($configurationArray, new ConfigRowDefinition());
        Assert::assertSame(null, $config->getData()['incrementalFetchingColumn']);
    }

    public function testMissingDbNode(): void
    {
        $configurationArray = [
            'parameters' => [
                'data_dir' => '/code/tests/Keboola/DbExtractor/../../data',
                'extractor_class' => 'MySQL',
            ],
        ];

        $exceptionMessage = 'The child config "db" under "root.parameters" must be configured.';
        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage($exceptionMessage);

        new Config($configurationArray, new ConfigRowDefinition());
    }

    /**
     * @return array
     */
    protected function getDbConfigurationArray(): array
    {
        return [
            'host' => 'mysql',
            'user' => 'root',
            '#password' => 'rootpassword',
            'database' => 'test',
            'port' => 3306,
            'ssl' => [
                '#key' => 'testKey',
                'ca' => 'testCa',
                'cert' => 'testCert',
                'cipher' => 'testCipher',
                'verifyServerCert' => false,
            ],
        ];
    }

    /**
     * @return array
     */
    protected function getExpectedDbConfigArray(): array
    {
        return [
            'host' => 'mysql',
            'user' => 'root',
            '#password' => 'rootpassword',
            'database' => 'test',
            'port' => 3306,
            'ssl' => [
                '#key' => 'testKey',
                'ca' => 'testCa',
                'cert' => 'testCert',
                'cipher' => 'testCipher',
                'verifyServerCert' => false,
                'enabled' => false,
                'ignoreCertificateCn' => false,
            ],
            'initQueries' => [],
        ];
    }
}
