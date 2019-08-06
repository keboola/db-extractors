<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Tests;

use Keboola\DbExtractorConfig\Config;
use Keboola\DbExtractorConfig\Configuration\ConfigRowDefinition;
use Keboola\DbExtractorConfig\Exception\UserException as ConfigUserException;
use Keboola\DbExtractorConfig\Test\AbstractConfigTest;

class ConfigTest extends AbstractConfigTest
{
    public const DRIVER = 'config';

    public function testInvalidConfigTableQuery(): void
    {
        $config = $this->getConfig(self::DRIVER);
        unset($config['parameters']['tables']);
        $config['parameters']['table'] = [
            'schema' => 'test',
            'tableName' => 'test',
        ];
        $config['parameters']['query'] = 'select 1 from test';
        $config['parameters']['outputTable'] = 'fake.output';

        $exceptionMessage = 'Invalid configuration for path "parameters": ';
        $exceptionMessage .= 'Both table and query cannot be set together.';
        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage($exceptionMessage);

        $Config = new Config(new ConfigRowDefinition());
        $Config->validateParameters($config['parameters']);
    }

    public function testInvalidConfigQueryIncremental(): void
    {
        $config = $this->getConfig(self::DRIVER);
        unset($config['parameters']['tables']);
        $config['parameters']['incrementalFetchingColumn'] = 'test';
        $config['parameters']['query'] = 'select 1 from test';
        $config['parameters']['outputTable'] = 'fake.output';

        $exceptionMessage = 'Invalid configuration for path "parameters": ';
        $exceptionMessage .= 'Incremental fetching is not supported for advanced queries.';
        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage($exceptionMessage);

        $Config = new Config(new ConfigRowDefinition());
        $Config->validateParameters($config['parameters']);
    }

    public function testInvalidConfigTableOrQuery(): void
    {
        $config = $this->getConfig(self::DRIVER);
        unset($config['parameters']['tables']);
        $config['parameters']['outputTable'] = 'fake.output';

        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage('Invalid configuration for path "parameters": One of table or query is required');

        $Config = new Config(new ConfigRowDefinition());
        $Config->validateParameters($config['parameters']);
    }

    public function testInvalidConfigsNeitherTableNorQueryWithNoName(): void
    {
        $config = $this->getConfigRow(self::DRIVER);
        unset($config['parameters']['query']);
        unset($config['parameters']['table']);

        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage('Invalid configuration for path "parameters": One of table or query is required');

        $Config = new Config(new ConfigRowDefinition());
        $Config->validateParameters($config['parameters']);
    }

    public function testInvalidConfigsInvalidTableWithNoName(): void
    {
        $config = $this->getConfigRow(self::DRIVER);
        unset($config['parameters']['name'], $config['parameters']['query']);
        $config['parameters']['table'] = ['tableName' => 'sales'];
        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage('The child node "schema" at path "parameters.table" must be configured.');

        $Config = new Config(new ConfigRowDefinition());
        $Config->validateParameters($config['parameters']);
    }

    public function testInvalidConfigsBothTableAndQueryWithNoName(): void
    {
        $config = $this->getConfigRow(self::DRIVER);
        $config['parameters']['table'] = ['tableName' => 'sales', 'schema' => 'test'];

        // we want to test the no results case
        $config['parameters']['query'] = 'SELECT 1 LIMIT 0';

        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage('Both table and query cannot be set together.');

        $Config = new Config(new ConfigRowDefinition());
        $Config->validateParameters($config['parameters']);
    }

    public function testInvalidConfigsBothIncrFetchAndQueryWithNoName(): void
    {
        $config = $this->getConfigRow(self::DRIVER);
        unset($config['parameters']['table']);
        $config['parameters']['incrementalFetchingColumn'] = 'abc';

        // we want to test the no results case
        $config['parameters']['query'] = 'SELECT 1 LIMIT 0';

        $exceptionMessage = 'Invalid configuration for path "parameters": ';
        $exceptionMessage .= 'Incremental fetching is not supported for advanced queries.';
        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage($exceptionMessage);

        $Config = new Config(new ConfigRowDefinition());
        $Config->validateParameters($config['parameters']);
    }
}
