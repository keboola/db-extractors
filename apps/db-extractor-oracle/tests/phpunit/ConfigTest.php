<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Configuration\NodeDefinition\OracleDbNode;
use Keboola\DbExtractorConfig\Config;
use Keboola\DbExtractorConfig\Configuration\ConfigRowDefinition;
use Keboola\DbExtractorConfig\Exception\UserException;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

class ConfigTest extends TestCase
{

    /**
     * @dataProvider validConfigDataProvider
     */
    public function testValidConfig(array $config): void
    {
        $outConfig = new Config($config, new ConfigRowDefinition(new OracleDbNode()));
        Assert::assertIsArray($outConfig->getData());
    }

    /**
     * @dataProvider invalidConfigDataProvider
     */
    public function testInvalidConfig(array $config, string $expectedMessage): void
    {
        $this->expectException(UserException::class);
        $this->expectExceptionMessage($expectedMessage);

        new Config($config, new ConfigRowDefinition(new OracleDbNode()));
    }

    public function validConfigDataProvider(): array
    {
        return [
            [
                [
                    'parameters' => [
                        'db' => [
                            'user' => 'testUser',
                            '#password' => 'testPassword',
                            'host' => 'testHost',
                            'port' => '1521',
                        ],
                        'data_dir' => 'test',
                        'extractor_class' => 'test',
                        'outputTable' => 'test',
                        'query' => 'test',
                    ],
                ],
            ],
            [
                [
                    'parameters' => [
                        'db' => [
                            'user' => 'testUser',
                            '#password' => 'testPassword',
                            'host' => 'testHost',
                        ],
                        'data_dir' => 'test',
                        'extractor_class' => 'test',
                        'outputTable' => 'test',
                        'query' => 'test',
                    ],
                ],
            ],
            [
                [
                    'parameters' => [
                        'db' => [
                            'user' => 'testUser',
                            '#password' => 'testPassword',
                            'tnsnames' => 'tnsnames file',
                        ],
                        'data_dir' => 'test',
                        'extractor_class' => 'test',
                        'outputTable' => 'test',
                        'query' => 'test',
                    ],
                ],
            ],
            [
                [
                    'parameters' => [
                        'db' => [
                            'user' => 'testUser',
                            '#password' => 'testPassword',
                            'tnsnames' => 'tnsnames file',
                            'defaultRowPrefetch' => '1000',
                        ],
                        'data_dir' => 'test',
                        'extractor_class' => 'test',
                        'outputTable' => 'test',
                        'query' => 'test',
                    ],
                ],
            ],
        ];
    }

    public function invalidConfigDataProvider(): array
    {
        return [
            [
                [
                    'parameters' => [
                        'db' => [
                            'user' => 'testUser',
                            '#password' => 'testPassword',
                            'host' => 'testHost',
                            'tnsnames' => 'tnsnames test content',
                        ],
                        'data_dir' => 'test',
                        'extractor_class' => 'test',
                        'outputTable' => 'test',
                        'query' => 'test',
                    ],
                ],
                'Tnsnames and host/port cannot be set together.',
            ],
            [
                [
                    'parameters' => [
                        'db' => [
                            'user' => 'testUser',
                            '#password' => 'testPassword',
                            'port' => '1521',
                            'tnsnames' => 'tnsnames test content',
                        ],
                        'data_dir' => 'test',
                        'extractor_class' => 'test',
                        'outputTable' => 'test',
                        'query' => 'test',
                    ],
                ],
                'Tnsnames and host/port cannot be set together.',
            ],
            [
                [
                    'parameters' => [
                        'db' => [
                            'user' => 'testUser',
                            '#password' => 'testPassword',
                            'host' => 'testHost',
                            'port' => '1521',
                            'tnsnames' => 'tnsnames test content',
                        ],
                        'data_dir' => 'test',
                        'extractor_class' => 'test',
                        'outputTable' => 'test',
                        'query' => 'test',
                    ],
                ],
                'Tnsnames and host/port cannot be set together.',
            ],
            [
                [
                    'parameters' => [
                        'db' => [
                            'user' => 'testUser',
                            '#password' => 'testPassword',
                        ],
                        'data_dir' => 'test',
                        'extractor_class' => 'test',
                        'outputTable' => 'test',
                        'query' => 'test',
                    ],
                ],
                'Host or tnsnames must be configured.',
            ],
        ];
    }
}
