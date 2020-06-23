<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Tests\ValueObject;

use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\SSLConnectionConfig;
use Keboola\DbExtractorConfig\Exception\PropertyNotSetException;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

class ExportConfigTest extends TestCase
{
    public function testTable(): void
    {
        $config = ExportConfig::fromArray([
            'table' => [
                'tableName' => 'table',
                'schema' => 'schema',
            ],
            'outputTable' => 'output-table',
            'retries' => 12,
            'columns' => [],
            'primaryKey' => [],
        ]);

        // Query
        Assert::assertSame(false, $config->hasQuery());
        try {
            $config->getQuery();
            Assert::fail('Exception is expected.');
        } catch (PropertyNotSetException $e) {
            // ok
        }

        // Table
        Assert::assertSame(true, $config->hasTable());
        Assert::assertSame('table', $config->getTable()->getName());
        Assert::assertSame('schema', $config->getTable()->getSchema());

        // Incremental loading
        Assert::assertSame(false, $config->isIncrementalLoading());

        // Incremental fetching
        Assert::assertSame(false, $config->isIncrementalFetching());
        try {
            $config->getIncrementalFetchingConfig();
            Assert::fail('Exception is expected.');
        } catch (PropertyNotSetException $e) {
            // ok
        }
        try {
            $config->getIncrementalFetchingColumn();
            Assert::fail('Exception is expected.');
        } catch (PropertyNotSetException $e) {
            // ok
        }
        try {
            $config->getIncrementalFetchingLimit();
            Assert::fail('Exception is expected.');
        } catch (PropertyNotSetException $e) {
            // ok
        }

        // Columns
        Assert::assertSame(false, $config->hasColumns());
        try {
            $config->getColumns();
            Assert::fail('Exception is expected.');
        } catch (PropertyNotSetException $e) {
            // ok
        }

        // Output table
        Assert::assertSame('output-table', $config->getOutputTable());

        // Primary key
        Assert::assertSame(false, $config->hasPrimaryKey());
        try {
            $config->getPrimaryKey();
            Assert::fail('Exception is expected.');
        } catch (PropertyNotSetException $e) {
            // ok
        }

        // Max retries
        Assert::assertSame(12, $config->getMaxRetries());

        // Config id
        try {
            $config->getConfigId();
            Assert::fail('Exception is expected.');
        } catch (PropertyNotSetException $e) {
            // ok
        }

        // Config name
        try {
            $config->getConfigName();
            Assert::fail('Exception is expected.');
        } catch (PropertyNotSetException $e) {
            // ok
        }
    }

    public function testColumns(): void
    {
        $config = ExportConfig::fromArray([
            'table' => [
                'tableName' => 'table',
                'schema' => 'schema',
            ],
            'outputTable' => 'output-table',
            'retries' => 12,
            'columns' => ['a', 'b', 'c'],
            'primaryKey' => [],
        ]);

        // Columns
        Assert::assertSame(true, $config->hasColumns());
        Assert::assertSame(['a', 'b', 'c'], $config->getColumns());
    }

    public function testPrimaryKey(): void
    {
        $config = ExportConfig::fromArray([
            'table' => [
                'tableName' => 'table',
                'schema' => 'schema',
            ],
            'outputTable' => 'output-table',
            'retries' => 12,
            'columns' => [],
            'primaryKey' => ['a', 'b', 'c'],
        ]);

        // Columns
        Assert::assertSame(true, $config->hasPrimaryKey());
        Assert::assertSame(['a', 'b', 'c'], $config->getPrimaryKey());
    }


    public function testQuery(): void
    {
        $config = ExportConfig::fromArray([
            'query' => 'SELECT * FROM `abc`',
            'outputTable' => 'output-table',
            'retries' => 12,
            'columns' => [],
            'primaryKey' => [],
        ]);

        // Query
        Assert::assertSame(true, $config->hasQuery());
        Assert::assertSame('SELECT * FROM `abc`', $config->getQuery());

        // Table
        Assert::assertSame(false, $config->hasTable());
        try {
            $config->getTable();
            Assert::fail('Exception is expected.');
        } catch (PropertyNotSetException $e) {
            // ok
        }

        // Incremental loading
        Assert::assertSame(false, $config->isIncrementalLoading());

        // Incremental fetching
        Assert::assertSame(false, $config->isIncrementalFetching());
        try {
            $config->getIncrementalFetchingConfig();
            Assert::fail('Exception is expected.');
        } catch (PropertyNotSetException $e) {
            // ok
        }
        try {
            $config->getIncrementalFetchingColumn();
            Assert::fail('Exception is expected.');
        } catch (PropertyNotSetException $e) {
            // ok
        }
        try {
            $config->getIncrementalFetchingLimit();
            Assert::fail('Exception is expected.');
        } catch (PropertyNotSetException $e) {
            // ok
        }

        // Columns
        Assert::assertSame(false, $config->hasColumns());
        try {
            $config->getColumns();
            Assert::fail('Exception is expected.');
        } catch (PropertyNotSetException $e) {
            // ok
        }

        // Output table
        Assert::assertSame('output-table', $config->getOutputTable());

        // Primary key
        Assert::assertSame(false, $config->hasPrimaryKey());
        try {
            $config->getPrimaryKey();
            Assert::fail('Exception is expected.');
        } catch (PropertyNotSetException $e) {
            // ok
        }

        // Max retries
        Assert::assertSame(12, $config->getMaxRetries());

        // Config id
        try {
            $config->getConfigId();
            Assert::fail('Exception is expected.');
        } catch (PropertyNotSetException $e) {
            // ok
        }

        // Config name
        try {
            $config->getConfigName();
            Assert::fail('Exception is expected.');
        } catch (PropertyNotSetException $e) {
            // ok
        }
    }

    public function testIncrementalFetching(): void
    {
        $config = ExportConfig::fromArray([
            'table' => [
                'tableName' => 'table',
                'schema' => 'schema',
            ],
            'outputTable' => 'output-table',
            'retries' => 12,
            'columns' => [],
            'primaryKey' => [],
            'incrementalFetchingColumn' => 'col123',
        ]);

        // Incremental loading
        Assert::assertSame(false, $config->isIncrementalLoading());

        // Incremental fetching
        Assert::assertSame(true, $config->isIncrementalFetching());
        Assert::assertSame('col123', $config->getIncrementalFetchingConfig()->getColumn());
        Assert::assertSame('col123', $config->getIncrementalFetchingColumn());

        Assert::assertFalse($config->hasIncrementalFetchingLimit());
        Assert::assertFalse($config->getIncrementalFetchingConfig()->hasLimit());

        try {
            $config->getIncrementalFetchingConfig()->getLimit();
            Assert::fail('Exception is expected.');
        } catch (PropertyNotSetException $e) {
            // ok
        }
        try {
            $config->getIncrementalFetchingLimit();
            Assert::fail('Exception is expected.');
        } catch (PropertyNotSetException $e) {
            // ok
        }
    }

    public function testIncrementalFetchingColumnNull(): void
    {
        $config = ExportConfig::fromArray([
            'table' => [
                'tableName' => 'table',
                'schema' => 'schema',
            ],
            'outputTable' => 'output-table',
            'retries' => 12,
            'columns' => [],
            'primaryKey' => [],
            'incrementalFetchingColumn' => null, // <<<<<<<<<<
        ]);
        Assert::assertFalse($config->isIncrementalFetching());
    }

    public function testIncrementalFetchingLimitNull(): void
    {
        $config = ExportConfig::fromArray([
            'table' => [
                'tableName' => 'table',
                'schema' => 'schema',
            ],
            'outputTable' => 'output-table',
            'retries' => 12,
            'columns' => [],
            'primaryKey' => [],
            'incrementalFetchingLimit' => null, // <<<<<<<<<<
        ]);
        Assert::assertFalse($config->isIncrementalFetching());
    }

    public function testIncrementalLoading(): void
    {
        $config = ExportConfig::fromArray([
            'table' => [
                'tableName' => 'table',
                'schema' => 'schema',
            ],
            'outputTable' => 'output-table',
            'retries' => 12,
            'columns' => [],
            'primaryKey' => [],
            'incremental' => true,
        ]);

        // Incremental loading
        Assert::assertSame(true, $config->isIncrementalLoading());

        // Incremental fetching
        Assert::assertSame(false, $config->isIncrementalFetching());
        try {
            $config->getIncrementalFetchingConfig();
            Assert::fail('Exception is expected.');
        } catch (PropertyNotSetException $e) {
            // ok
        }
        try {
            $config->getIncrementalFetchingColumn();
            Assert::fail('Exception is expected.');
        } catch (PropertyNotSetException $e) {
            // ok
        }
        try {
            $config->getIncrementalFetchingLimit();
            Assert::fail('Exception is expected.');
        } catch (PropertyNotSetException $e) {
            // ok
        }
    }

    public function testIncrementalFetchingAndLoading(): void
    {
        $config = ExportConfig::fromArray([
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
            'incrementalFetchingLimit' => 456,
        ]);

        // Incremental loading
        Assert::assertSame(true, $config->isIncrementalLoading());

        // Incremental fetching
        Assert::assertSame(true, $config->isIncrementalFetching());
        Assert::assertSame('col123', $config->getIncrementalFetchingConfig()->getColumn());
        Assert::assertSame('col123', $config->getIncrementalFetchingColumn());
        Assert::assertSame(456, $config->getIncrementalFetchingConfig()->getLimit());
        Assert::assertSame(456, $config->getIncrementalFetchingLimit());
    }

    public function testIncrementalFetchingWithLimit(): void
    {
        $config = ExportConfig::fromArray([
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
            'incrementalFetchingLimit' => 456,
        ]);

        Assert::assertTrue($config->hasIncrementalFetchingLimit());
        Assert::assertTrue($config->getIncrementalFetchingConfig()->hasLimit());

        // Incremental fetching
        Assert::assertSame(true, $config->isIncrementalFetching());
        Assert::assertSame('col123', $config->getIncrementalFetchingConfig()->getColumn());
        Assert::assertSame('col123', $config->getIncrementalFetchingColumn());
        Assert::assertSame(456, $config->getIncrementalFetchingConfig()->getLimit());
        Assert::assertSame(456, $config->getIncrementalFetchingLimit());
    }

    public function testConfigIdAndName(): void
    {
        $config = ExportConfig::fromArray([
            'id' => 123,
            'name' => 'my config name',
            'table' => [
                'tableName' => 'table',
                'schema' => 'schema',
            ],
            'outputTable' => 'output-table',
            'retries' => 12,
            'columns' => [],
            'primaryKey' => [],
            'incremental' => false,
        ]);

        Assert::assertTrue($config->hasConfigId());
        Assert::assertTrue($config->hasConfigName());
        Assert::assertSame(123, $config->getConfigId());
        Assert::assertSame('my config name', $config->getConfigName());
    }

    public function testSslConnectionConfig(): void
    {
        $config = ExportConfig::fromArray([
            'table' => [
                'tableName' => 'table',
                'schema' => 'schema',
            ],
            'db' => [
                'ssl' => [
                    'ca' => 'testCa',
                    'cert' => 'testCert',
                    'cipher' => 'testCipher',
                    'key' => 'testKey',
                ],
            ],
            'outputTable' => 'output-table',
            'retries' => 12,
            'columns' => [],
            'primaryKey' => [],
        ]);

        Assert::assertTrue($config->isSSlConnection());

        Assert::assertInstanceOf(SSLConnectionConfig::class, $config->getSslConnectionConfig());

        Assert::assertEquals('testCa', $config->getSslConnectionConfig()->getCa());
        Assert::assertEquals('testCert', $config->getSslConnectionConfig()->getCert());
        Assert::assertEquals('testCipher', $config->getSslConnectionConfig()->getCipher());
        Assert::assertEquals('testKey', $config->getSslConnectionConfig()->getKey());
    }
}
