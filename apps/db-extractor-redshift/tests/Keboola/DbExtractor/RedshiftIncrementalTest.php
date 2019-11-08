<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Application;

class RedshiftIncrementalTest extends AbstractRedshiftTest
{

    public function testIncrementalFetchingByTimestamp(): void
    {
        $config = $this->getConfigRow();
        $config['parameters']['incrementalFetchingColumn'] = 'timestamp';
        $config['parameters']['table']['tableName'] = 'auto_increment_timestamp';
        $config['parameters']['outputTable'] = 'in.c-main.auto-increment-timestamp';
        $config['parameters']['columns'] = [];
        $this->createAutoIncrementAndTimestampTable($config);
        $app = $this->createApplication($config);

        $result = $app->run();
        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2,
            ],
            $result['imported']
        );

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertNotEmpty($result['state']['lastFetchedRow']);

        sleep(2);
        // the next fetch should be empty
        $noNewRowsResult = ($this->createApplication($config, $result['state']))->run();
        $this->assertEquals(1, $noNewRowsResult['imported']['rows']);

        $this->insertRowToTable($config, ['weird-Name' => 'charles']);
        $this->insertRowToTable($config, ['weird-Name' => 'william']);

        $newResult = ($this->createApplication($config, $result['state']))->run();

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertGreaterThan(
            $result['state']['lastFetchedRow'],
            $newResult['state']['lastFetchedRow']
        );
        $this->assertEquals(3, $newResult['imported']['rows']);
    }

    public function testIncrementalFetchingByDatetime(): void
    {
        $config = $this->getConfigRow();
        $config['parameters']['incrementalFetchingColumn'] = 'datetime';
        $config['parameters']['table']['tableName'] = 'auto_increment_datetime';
        $config['parameters']['outputTable'] = 'in.c-main.auto-increment-datetime';
        $config['parameters']['columns'] = [];
        $this->createAutoIncrementAndTimestampTable($config);
        $app = $this->createApplication($config);

        $result = $app->run();
        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-datetime',
                'rows' => 2,
            ],
            $result['imported']
        );

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertNotEmpty($result['state']['lastFetchedRow']);

        sleep(2);
        // the next fetch should be empty
        $noNewRowsResult = ($this->createApplication($config, $result['state']))->run();
        $this->assertEquals(1, $noNewRowsResult['imported']['rows']);

        $this->insertRowToTable($config, ['weird-Name' => 'charles']);
        $this->insertRowToTable($config, ['weird-Name' => 'william']);

        $newResult = ($this->createApplication($config, $result['state']))->run();

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertGreaterThan(
            $result['state']['lastFetchedRow'],
            $newResult['state']['lastFetchedRow']
        );
        $this->assertEquals(3, $newResult['imported']['rows']);
    }

    public function testIncrementalFetchingByAutoIncrement(): void
    {
        $config = $this->getConfigRow();
        $config['parameters']['incrementalFetchingColumn'] = '_weird-i-d';
        $config['parameters']['table']['tableName'] = 'auto_increment_autoincrement';
        $config['parameters']['outputTable'] = 'in.c-main.auto-increment-autoincrement';
        $config['parameters']['columns'] = [];
        $this->createAutoIncrementAndTimestampTable($config);
        $app = $this->createApplication($config);

        $result = $app->run();
        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-autoincrement',
                'rows' => 2,
            ],
            $result['imported']
        );

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(2, $result['state']['lastFetchedRow']);

        sleep(2);
        // the next fetch should be empty
        $noNewRowsResult = ($this->createApplication($config, $result['state']))->run();
        $this->assertEquals(1, $noNewRowsResult['imported']['rows']);

        sleep(2);
        //now add a couple rows and run it again.
        $this->insertRowToTable($config, ['weird-Name' => 'charles']);
        $this->insertRowToTable($config, ['weird-Name' => 'william']);

        $newResult = ($this->createApplication($config, $result['state']))->run();

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertEquals(4, $newResult['state']['lastFetchedRow']);
        $this->assertEquals(3, $newResult['imported']['rows']);
    }

    public function testIncrementalFetchingByInteger(): void
    {
        $config = $this->getConfigRow();
        $config['parameters']['incrementalFetchingColumn'] = 'intcolumn';
        $config['parameters']['table']['tableName'] = 'auto_increment_intcolumn';
        $config['parameters']['outputTable'] = 'in.c-main.auto-increment-intcolumn';
        $config['parameters']['columns'] = [];
        $this->createAutoIncrementAndTimestampTable($config);
        $app = $this->createApplication($config);

        $result = $app->run();
        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-intcolumn',
                'rows' => 2,
            ],
            $result['imported']
        );

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(3, $result['state']['lastFetchedRow']);

        sleep(2);
        // the next fetch should be empty
        $noNewRowsResult = ($this->createApplication($config, $result['state']))->run();
        $this->assertEquals(1, $noNewRowsResult['imported']['rows']);

        sleep(2);
        //now add a couple rows and run it again.
        $this->insertRowToTable($config, ['weird-Name' => 'charles', 'intColumn' => 4]);
        $this->insertRowToTable($config, ['weird-Name' => 'william', 'intColumn' => 7]);

        $newResult = ($this->createApplication($config, $result['state']))->run();

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertEquals(7, $newResult['state']['lastFetchedRow']);
        $this->assertEquals(3, $newResult['imported']['rows']);
    }

    public function testIncrementalFetchingByDecimal(): void
    {
        $config = $this->getConfigRow();
        $config['parameters']['incrementalFetchingColumn'] = 'decimalcolumn';
        $config['parameters']['table']['tableName'] = 'auto_increment_decimalcolumn';
        $config['parameters']['outputTable'] = 'in.c-main.auto-increment-decimalcolumn';
        $config['parameters']['columns'] = [];
        $this->createAutoIncrementAndTimestampTable($config);
        $app = $this->createApplication($config);

        $result = $app->run();
        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-decimalcolumn',
                'rows' => 2,
            ],
            $result['imported']
        );

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(30.3, $result['state']['lastFetchedRow']);

        sleep(2);
        // the next fetch should be empty
        $noNewRowsResult = ($this->createApplication($config, $result['state']))->run();
        $this->assertEquals(1, $noNewRowsResult['imported']['rows']);

        sleep(2);
        //now add a couple rows and run it again.  Only the one row that has decimal >= to 30.3 should be included
        $this->insertRowToTable($config, ['weird-Name' => 'charles', 'decimalColumn' => 4.4]);
        $this->insertRowToTable($config, ['weird-Name' => 'william', 'decimalColumn' => 70.7]);

        $newResult = ($this->createApplication($config, $result['state']))->run();

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertEquals(70.7, $newResult['state']['lastFetchedRow']);
        $this->assertEquals(2, $newResult['imported']['rows']);
    }

    public function testIncrementalFetchingLimit(): void
    {
        $config = $this->getConfigRow();
        $config['parameters']['incrementalFetchingColumn'] = '_weird-i-d';
        $config['parameters']['incrementalFetchingLimit'] = 1;
        $config['parameters']['table']['tableName'] = 'auto_increment_autoincrement';
        $config['parameters']['outputTable'] = 'in.c-main.auto-increment-autoincrement';
        $config['parameters']['columns'] = [];
        $this->createAutoIncrementAndTimestampTable($config);
        $app = $this->createApplication($config);

        $result = $app->run();
        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-autoincrement',
                'rows' => 1,
            ],
            $result['imported']
        );

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(1, $result['state']['lastFetchedRow']);

        sleep(2);
        // for the next fetch should contain the second row the limit must be 2 since we are using >=
        $config['parameters']['incrementalFetchingLimit'] = 2;
        $result = ($this->createApplication($config, $result['state']))->run();
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-autoincrement',
                'rows' => 2,
            ],
            $result['imported']
        );

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(2, $result['state']['lastFetchedRow']);
    }
}
