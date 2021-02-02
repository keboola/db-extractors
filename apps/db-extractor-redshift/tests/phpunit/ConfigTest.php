<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Generator;
use Keboola\CommonExceptions\UserExceptionInterface;
use Keboola\DbExtractor\RedshiftApplication;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

class ConfigTest extends TestCase
{
    /**
     * @dataProvider validConfigProvider
     */
    public function testValid(array $config): void
    {
        new RedshiftApplication($config, new TestLogger(), [], 'testDataDir');
        $this->expectNotToPerformAssertions();
    }

    /**
     * @dataProvider invalidConfigProvider
     */
    public function testInvalid(array $config, string $expectedMessage): void
    {
        $this->expectException(UserExceptionInterface::class);
        $this->expectExceptionMessage($expectedMessage);
        new RedshiftApplication($config, new TestLogger(), [], 'testDataDir');
    }

    public function validConfigProvider(): Generator
    {
        yield [
            [
                'parameters' => [
                    'db' => [
                        'host' => 'redshift',
                        'user' => 'root',
                        '#password' => 'rootpassword',
                        'port' => 3306,
                    ],
                    'query' => 'SELECT * FROM escaping',
                    'outputTable' => 'in.c-main.escaping',
                ],
            ],
        ];
        yield [
            [
                'parameters' => [
                    'db' => [
                        'host' => 'redshift',
                        'user' => 'root',
                        '#password' => 'rootpassword',
                        'database' => 'redshiftDatabase',
                        'port' => 3306,
                    ],
                    'query' => 'SELECT * FROM escaping',
                    'outputTable' => 'in.c-main.escaping',
                ],
            ],
        ];
    }

    public function invalidConfigProvider(): Generator
    {
        yield [
            [
                'parameters' => [
                    'db' => [
                        'host' => 'redshift',
                        'user' => 'root',
                        '#password' => 'rootpassword',
                        'database' => '',
                        'port' => 3306,
                    ],
                    'query' => 'SELECT * FROM escaping',
                    'outputTable' => 'in.c-main.escaping',
                ],
            ],
            'The path "root.parameters.db.database" cannot contain an empty value, but got "".',
        ];

        yield [
            [
                'parameters' => [
                    'db' => [
                        'host' => 'redshift',
                        'user' => 'root',
                        '#password' => 'rootpassword',
                        'database' => 'redshiftDatabase',
                        'port' => 3306,
                    ],
                    'query' => 'SELECT * FROM escaping',
                    'table' => [
                        'schema' => 'schema',
                        'tableName' => 'tableName',
                    ],
                    'outputTable' => 'in.c-main.escaping',
                ],
            ],
            'Both table and query cannot be set together.',
        ];

        yield [
            [
                'parameters' => [
                    'db' => [
                        'host' => 'redshift',
                        'user' => 'root',
                        '#password' => 'rootpassword',
                        'database' => 'redshiftDatabase',
                        'port' => 3306,
                    ],
                    'table' => [],
                    'outputTable' => 'in.c-main.escaping',
                ],
            ],
            'The child config "schema" under "root.parameters.table" must be configured.',
        ];

        yield [
            [
                'parameters' => [
                    'db' => [
                        'host' => 'redshift',
                        'user' => 'root',
                        '#password' => 'rootpassword',
                        'database' => 'redshiftDatabase',
                        'port' => 3306,
                    ],
                    'table' => [
                        'schema' => 'schema',
                    ],
                    'outputTable' => 'in.c-main.escaping',
                ],
            ],
            'The child config "tableName" under "root.parameters.table" must be configured.',
        ];

        yield [
            [
                'parameters' => [
                    'db' => [
                        'host' => 'redshift',
                        'user' => 'root',
                        '#password' => 'rootpassword',
                        'database' => 'redshiftDatabase',
                        'port' => 3306,
                    ],
                    'query' => 'SELECT * FROM escaping',
                    'incrementalFetchingColumn' => 'id',
                    'outputTable' => 'in.c-main.escaping',
                ],
            ],
            'The "incrementalFetchingColumn" is configured, ' .
            'but incremental fetching is not supported for custom query.',
        ];
    }
}
