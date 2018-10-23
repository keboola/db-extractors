<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\MySQLApplication;
use Keboola\DbExtractor\Exception\UserException;
use Symfony\Component\Yaml\Yaml;
use Nette\Utils;

class MySQLTest extends AbstractMySQLTest
{
    public function testCredentials(): void
    {
        $config = $this->getConfig();

        $config['action'] = 'testConnection';
        unset($config['parameters']['tables']);
        $config['parameters']['db']['networkCompression'] = true;
        
        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    public function testCredentialsWithoutDatabase(): void
    {
        $config = $this->getConfig();
        $config['action'] = 'testConnection';
        $config['parameters']['tables'] = [];
        unset($config['parameters']['db']['database']);

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    public function testRunWithoutTables(): void
    {
        $config = $this->getConfig();

        $config['parameters']['tables'] = [];

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    /**
     * @param $configType
     * @dataProvider configTypesProvider
     */
    public function testRunMain(string $configType): void
    {
        $config = $this->getConfig(self::DRIVER, $configType);
        $app = $this->createApplication($config);

        $csv1 = new CsvFile($this->dataDir . '/mysql/sales.csv');
        $this->createTextTable($csv1);

        $csv2 = new CsvFile($this->dataDir . '/mysql/escaping.csv');
        $this->createTextTable($csv2);

        $result = $app->run();

        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv';

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv.manifest');
        $this->assertFileEquals((string) $csv1, $outputCsvFile);


        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][1]['outputTable'] . '.csv';

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($this->dataDir . '/out/tables/' . $result['imported'][1]['outputTable'] . '.csv.manifest');
        $this->assertFileEquals((string) $csv2, $outputCsvFile);
    }

    public function testRunWithoutDatabase(): void
    {
        $config = $this->getConfig();
        $config['action'] = 'testConnection';
        unset($config['parameters']['db']['database']);

        // Add schema to db query
        $config['parameters']['tables'][0]['query'] = "SELECT * FROM test.sales";
        $config['parameters']['tables'][1]['query'] = "SELECT * FROM test.escaping";

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    public function testCredentialsWithSSH(): void
    {
        $config = $this->getConfig();
        $config['action'] = 'testConnection';

        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getPrivateKey('mysql'),
                'public' => $this->getEnv('mysql', 'DB_SSH_KEY_PUBLIC'),
            ],
            'user' => 'root',
            'sshHost' => 'sshproxy',
            'sshPort' => '22',
            'remoteHost' => 'mysql',
            'remotePort' => $this->getEnv('mysql', 'DB_PORT'),
            'localPort' => '23305',
        ];

        $app = $this->createApplication($config);

        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    public function testRunWithSSH(): void
    {
        $config = $this->getConfig();
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getPrivateKey('mysql'),
                'public' => $this->getEnv('mysql', 'DB_SSH_KEY_PUBLIC'),
            ],
            'user' => 'root',
            'sshHost' => 'sshproxy',
            'localPort' => '23306',
        ];

        $app = $this->createApplication($config);

        $csv1 = new CsvFile($this->dataDir . '/mysql/sales.csv');
        $this->createTextTable($csv1);

        $csv2 = new CsvFile($this->dataDir . '/mysql/escaping.csv');
        $this->createTextTable($csv2);

        $result = $app->run();

        $sanitizedTable = Utils\Strings::webalize($result['imported'][0]['outputTable'], '._');
        $outputCsvFile = $this->dataDir . '/out/tables/' . $sanitizedTable . '.csv';

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($this->dataDir . '/out/tables/' . $sanitizedTable . '.csv.manifest');
        $this->assertFileEquals((string) $csv1, $outputCsvFile);

        $sanitizedTable = Utils\Strings::webalize($result['imported'][1]['outputTable'], '._');
        $outputCsvFile = $this->dataDir . '/out/tables/' . $sanitizedTable . '.csv';

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($this->dataDir . '/out/tables/' . $sanitizedTable . '.csv.manifest');
        $this->assertFileEquals((string) $csv2, $outputCsvFile);
    }

    public function testUserException(): void
    {
        $this->setExpectedException('Keboola\DbExtractor\Exception\UserException');

        $config = $this->getConfig('mysql');

        $config['parameters']['db']['host'] = 'nonexistinghost';
        $app = $this->createApplication($config);

        $app->run();
    }

    public function testGetTables(): void
    {
        $this->createAutoIncrementAndTimestampTable();

        // add a table to a different schema (should not be fetched)
        $this->createTextTable(
            new CsvFile($this->dataDir . '/mysql/sales.csv'),
            "ext_sales",
            "temp_schema"
        );

        $config = $this->getConfig();
        $config['action'] = 'getTables';
        $app = $this->createApplication($config);

        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertArrayHasKey('tables', $result);

        $this->assertEquals('success', $result['status']);
        $this->assertCount(3, $result['tables']);
        
        $expectedData = array (
            0 =>
                array (
                    'name' => 'auto_increment_timestamp',
                    'schema' => 'test',
                    'type' => 'BASE TABLE',
                    'rowCount' => '2',
                    'autoIncrement' => '3',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => '_weird-I-d',
                                    'sanitizedName' => 'weird_I_d',
                                    'type' => 'int',
                                    'primaryKey' => true,
                                    'length' => '10',
                                    'nullable' => false,
                                    'default' => null,
                                    'ordinalPosition' => '1',
                                    'extra' => 'auto_increment',
                                    'autoIncrement' => '3',
                                    'description' => 'This is a weird ID',
                                ),
                            1 =>
                                array (
                                    'name' => 'weird-Name',
                                    'sanitizedName' => 'weird_Name',
                                    'type' => 'varchar',
                                    'primaryKey' => false,
                                    'length' => '30',
                                    'nullable' => false,
                                    'default' => 'pam',
                                    'ordinalPosition' => '2',
                                    'description' => 'This is a weird name',
                                ),
                            2 =>
                                array (
                                    'name' => 'timestamp',
                                    'sanitizedName' => 'timestamp',
                                    'type' => 'timestamp',
                                    'primaryKey' => false,
                                    'length' => null,
                                    'nullable' => false,
                                    'default' => 'CURRENT_TIMESTAMP',
                                    'ordinalPosition' => '3',
                                    'extra' => 'on update CURRENT_TIMESTAMP',
                                    'description' => 'This is a timestamp',
                                ),
                        ),
                    'timestampUpdateColumn' => 'timestamp',
                    'description' => 'This is a table comment',
                ),
            1 =>
                array (
                    'name' => 'escaping',
                    'schema' => 'test',
                    'type' => 'BASE TABLE',
                    'rowCount' => '7',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'col1',
                                    'sanitizedName' => 'col1',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '1',
                                ),
                            1 =>
                                array (
                                    'name' => 'col2',
                                    'sanitizedName' => 'col2',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '2',
                                ),
                        ),
                ),
            2 =>
                array (
                    'name' => 'sales',
                    'schema' => 'test',
                    'type' => 'BASE TABLE',
                    'rowCount' => '100',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'usergender',
                                    'sanitizedName' => 'usergender',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '1',
                                ),
                            1 =>
                                array (
                                    'name' => 'usercity',
                                    'sanitizedName' => 'usercity',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '2',
                                ),
                            2 =>
                                array (
                                    'name' => 'usersentiment',
                                    'sanitizedName' => 'usersentiment',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '3',
                                ),
                            3 =>
                                array (
                                    'name' => 'zipcode',
                                    'sanitizedName' => 'zipcode',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '4',
                                ),
                            4 =>
                                array (
                                    'name' => 'sku',
                                    'sanitizedName' => 'sku',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '5',
                                ),
                            5 =>
                                array (
                                    'name' => 'createdat',
                                    'sanitizedName' => 'createdat',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '6',
                                ),
                            6 =>
                                array (
                                    'name' => 'category',
                                    'sanitizedName' => 'category',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '7',
                                ),
                            7 =>
                                array (
                                    'name' => 'price',
                                    'sanitizedName' => 'price',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '8',
                                ),
                            8 =>
                                array (
                                    'name' => 'county',
                                    'sanitizedName' => 'county',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '9',
                                ),
                            9 =>
                                array (
                                    'name' => 'countycode',
                                    'sanitizedName' => 'countycode',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '10',
                                ),
                            10 =>
                                array (
                                    'name' => 'userstate',
                                    'sanitizedName' => 'userstate',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '11',
                                ),
                            11 =>
                                array (
                                    'name' => 'categorygroup',
                                    'sanitizedName' => 'categorygroup',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '12',
                                ),
                        ),
                ),
        );
        $this->assertEquals($expectedData, $result['tables']);
    }

    public function testGetTablesNoDatabase(): void
    {
        $this->createAutoIncrementAndTimestampTable();

        // add a table to a different schema
        $this->createTextTable(
            new CsvFile($this->dataDir . '/mysql/sales.csv'),
            "ext_sales",
            "temp_schema"
        );

        $config = $this->getConfig();
        $config['parameters']['tables'] = [];
        unset($config['parameters']['db']['database']);
        $config['action'] = 'getTables';
        $app = $this->createApplication($config);

        $result = $app->run();

        $this->assertGreaterThanOrEqual(4, count($result['tables']));

        $expectedTables = array (
            0 =>
                array (
                    'name' => 'ext_sales',
                    'schema' => 'temp_schema',
                    'type' => 'BASE TABLE',
                    'rowCount' => '100',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'usergender',
                                    'sanitizedName' => 'usergender',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '1',
                                ),
                            1 =>
                                array (
                                    'name' => 'usercity',
                                    'sanitizedName' => 'usercity',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '2',
                                ),
                            2 =>
                                array (
                                    'name' => 'usersentiment',
                                    'sanitizedName' => 'usersentiment',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '3',
                                ),
                            3 =>
                                array (
                                    'name' => 'zipcode',
                                    'sanitizedName' => 'zipcode',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '4',
                                ),
                            4 =>
                                array (
                                    'name' => 'sku',
                                    'sanitizedName' => 'sku',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '5',
                                ),
                            5 =>
                                array (
                                    'name' => 'createdat',
                                    'sanitizedName' => 'createdat',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '6',
                                ),
                            6 =>
                                array (
                                    'name' => 'category',
                                    'sanitizedName' => 'category',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '7',
                                ),
                            7 =>
                                array (
                                    'name' => 'price',
                                    'sanitizedName' => 'price',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '8',
                                ),
                            8 =>
                                array (
                                    'name' => 'county',
                                    'sanitizedName' => 'county',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '9',
                                ),
                            9 =>
                                array (
                                    'name' => 'countycode',
                                    'sanitizedName' => 'countycode',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '10',
                                ),
                            10 =>
                                array (
                                    'name' => 'userstate',
                                    'sanitizedName' => 'userstate',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '11',
                                ),
                            11 =>
                                array (
                                    'name' => 'categorygroup',
                                    'sanitizedName' => 'categorygroup',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '12',
                                ),
                        ),
                ),
            1 =>
                array (
                    'name' => 'auto_increment_timestamp',
                    'schema' => 'test',
                    'type' => 'BASE TABLE',
                    'rowCount' => '2',
                    'autoIncrement' => '3',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => '_weird-I-d',
                                    'sanitizedName' => 'weird_I_d',
                                    'type' => 'int',
                                    'primaryKey' => true,
                                    'length' => '10',
                                    'nullable' => false,
                                    'default' => null,
                                    'ordinalPosition' => '1',
                                    'extra' => 'auto_increment',
                                    'autoIncrement' => '3',
                                    'description' => 'This is a weird ID',
                                ),
                            1 =>
                                array (
                                    'name' => 'weird-Name',
                                    'sanitizedName' => 'weird_Name',
                                    'type' => 'varchar',
                                    'primaryKey' => false,
                                    'length' => '30',
                                    'nullable' => false,
                                    'default' => 'pam',
                                    'ordinalPosition' => '2',
                                    'description' => 'This is a weird name',
                                ),
                            2 =>
                                array (
                                    'name' => 'timestamp',
                                    'sanitizedName' => 'timestamp',
                                    'type' => 'timestamp',
                                    'primaryKey' => false,
                                    'length' => null,
                                    'nullable' => false,
                                    'default' => 'CURRENT_TIMESTAMP',
                                    'ordinalPosition' => '3',
                                    'extra' => 'on update CURRENT_TIMESTAMP',
                                    'description' => 'This is a timestamp',
                                ),
                        ),
                    'timestampUpdateColumn' => 'timestamp',
                    'description' => 'This is a table comment',
                ),
            2 =>
                array (
                    'name' => 'escaping',
                    'schema' => 'test',
                    'type' => 'BASE TABLE',
                    'rowCount' => '7',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'col1',
                                    'sanitizedName' => 'col1',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '1',
                                ),
                            1 =>
                                array (
                                    'name' => 'col2',
                                    'sanitizedName' => 'col2',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '2',
                                ),
                        ),
                ),
            3 =>
                array (
                    'name' => 'sales',
                    'schema' => 'test',
                    'type' => 'BASE TABLE',
                    'rowCount' => '100',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'usergender',
                                    'sanitizedName' => 'usergender',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '1',
                                ),
                            1 =>
                                array (
                                    'name' => 'usercity',
                                    'sanitizedName' => 'usercity',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '2',
                                ),
                            2 =>
                                array (
                                    'name' => 'usersentiment',
                                    'sanitizedName' => 'usersentiment',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '3',
                                ),
                            3 =>
                                array (
                                    'name' => 'zipcode',
                                    'sanitizedName' => 'zipcode',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '4',
                                ),
                            4 =>
                                array (
                                    'name' => 'sku',
                                    'sanitizedName' => 'sku',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '5',
                                ),
                            5 =>
                                array (
                                    'name' => 'createdat',
                                    'sanitizedName' => 'createdat',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '6',
                                ),
                            6 =>
                                array (
                                    'name' => 'category',
                                    'sanitizedName' => 'category',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '7',
                                ),
                            7 =>
                                array (
                                    'name' => 'price',
                                    'sanitizedName' => 'price',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '8',
                                ),
                            8 =>
                                array (
                                    'name' => 'county',
                                    'sanitizedName' => 'county',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '9',
                                ),
                            9 =>
                                array (
                                    'name' => 'countycode',
                                    'sanitizedName' => 'countycode',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '10',
                                ),
                            10 =>
                                array (
                                    'name' => 'userstate',
                                    'sanitizedName' => 'userstate',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '11',
                                ),
                            11 =>
                                array (
                                    'name' => 'categorygroup',
                                    'sanitizedName' => 'categorygroup',
                                    'type' => 'text',
                                    'primaryKey' => false,
                                    'length' => '65535',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '12',
                                ),
                        ),
                ),
        );
        $this->assertEquals($expectedTables, $result['tables']);
    }

    /**
     * @dataProvider configProvider
     */
    public function testManifestMetadata(array $config): void
    {
        $isConfigRow = !isset($config['parameters']['tables']);

        $tableParams = ($isConfigRow) ? $config['parameters'] : $config['parameters']['tables'][0];
        unset($tableParams['query']);
        $tableParams['outputTable'] = 'in.c-main.foreignkey';
        $tableParams['primaryKey'] = ['some_primary_key'];
        $tableParams['table'] = [
            'tableName' => 'auto_increment_timestamp_withFK',
            'schema' => 'test',
        ];
        if ($isConfigRow) {
            $config['parameters'] = $tableParams;
        } else {
            $config['parameters']['tables'][0] = $tableParams;
            unset($config['parameters']['tables'][1]);
            unset($config['parameters']['tables'][2]);
        }

        $this->createAutoIncrementAndTimestampTable();
        $this->createAutoIncrementAndTimestampTableWithFK();

        $app = $this->createApplication($config);

        $result = $app->run();

        $importedTable = ($isConfigRow) ? $result['imported']['outputTable'] : $result['imported'][0]['outputTable'];

        $sanitizedTable = Utils\Strings::webalize($importedTable, '._');
        $outputManifest = json_decode(
            file_get_contents($this->dataDir . '/out/tables/' . $sanitizedTable . '.csv.manifest'),
            true
        );

        $this->assertArrayHasKey('destination', $outputManifest);
        $this->assertArrayHasKey('incremental', $outputManifest);
        $this->assertArrayHasKey('metadata', $outputManifest);
        $expectedMetadata = [
            'KBC.name' => 'auto_increment_timestamp_withFK',
            'KBC.schema' => 'test',
            'KBC.type' => 'BASE TABLE',
            'KBC.rowCount' => 1,
            'KBC.description' => 'This is a table comment',
            'KBC.autoIncrement' => '2',
        ];
        $tableMetadata = [];
        foreach ($outputManifest['metadata'] as $i => $metadata) {
            $this->assertArrayHasKey('key', $metadata);
            $this->assertArrayHasKey('value', $metadata);
            $tableMetadata[$metadata['key']] = $metadata['value'];
        }
        $this->assertEquals($expectedMetadata, $tableMetadata);

        $this->assertArrayHasKey('column_metadata', $outputManifest);
        $this->assertCount(4, $outputManifest['column_metadata']);

        $expectedColumnMetadata = array (
            'some_primary_key' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'int',
                        ),
                    1 =>
                        array (
                            'key' => 'KBC.datatype.nullable',
                            'value' => false,
                        ),
                    2 =>
                        array (
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'INTEGER',
                        ),
                    3 =>
                        array (
                            'key' => 'KBC.datatype.length',
                            'value' => '10',
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.sourceName',
                            'value' => 'some_primary_key',
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.sanitizedName',
                            'value' => 'some_primary_key',
                        ),
                    6 =>
                        array (
                            'key' => 'KBC.primaryKey',
                            'value' => true,
                        ),
                    7 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => '1',
                        ),
                    8 =>
                        array (
                            'key' => 'KBC.description',
                            'value' => 'This is a weird ID',
                        ),
                    9 =>
                        array (
                            'key' => 'KBC.extra',
                            'value' => 'auto_increment',
                        ),
                    10 =>
                        array (
                            'key' => 'KBC.autoIncrement',
                            'value' => '2',
                        ),
                    11 =>
                        array (
                            'key' => 'KBC.constraintName',
                            'value' => 'PRIMARY',
                        ),
                ),
            'random_name' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'varchar',
                        ),
                    1 =>
                        array (
                            'key' => 'KBC.datatype.nullable',
                            'value' => false,
                        ),
                    2 =>
                        array (
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'STRING',
                        ),
                    3 =>
                        array (
                            'key' => 'KBC.datatype.length',
                            'value' => '30',
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.datatype.default',
                            'value' => 'pam',
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.sourceName',
                            'value' => 'random_name',
                        ),
                    6 =>
                        array (
                            'key' => 'KBC.sanitizedName',
                            'value' => 'random_name',
                        ),
                    7 =>
                        array (
                            'key' => 'KBC.primaryKey',
                            'value' => false,
                        ),
                    8 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => '2',
                        ),
                    9 =>
                        array (
                            'key' => 'KBC.description',
                            'value' => 'This is a weird name',
                        ),
                ),
            'datetime' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'datetime',
                        ),
                    1 =>
                        array (
                            'key' => 'KBC.datatype.nullable',
                            'value' => true,
                        ),
                    2 =>
                        array (
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'TIMESTAMP',
                        ),
                    3 =>
                        array (
                            'key' => 'KBC.datatype.default',
                            'value' => 'CURRENT_TIMESTAMP',
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.sourceName',
                            'value' => 'datetime',
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.sanitizedName',
                            'value' => 'datetime',
                        ),
                    6 =>
                        array (
                            'key' => 'KBC.primaryKey',
                            'value' => false,
                        ),
                    7 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => '3',
                        ),
                ),
            'foreign_key' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'int',
                        ),
                    1 =>
                        array (
                            'key' => 'KBC.datatype.nullable',
                            'value' => true,
                        ),
                    2 =>
                        array (
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'INTEGER',
                        ),
                    3 =>
                        array (
                            'key' => 'KBC.datatype.length',
                            'value' => '10',
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.sourceName',
                            'value' => 'foreign_key',
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.sanitizedName',
                            'value' => 'foreign_key',
                        ),
                    6 =>
                        array (
                            'key' => 'KBC.primaryKey',
                            'value' => false,
                        ),
                    7 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => '4',
                        ),
                    8 =>
                        array (
                            'key' => 'KBC.description',
                            'value' => 'This is a foreign key',
                        ),
                    9 =>
                        array (
                            'key' => 'KBC.constraintName',
                            'value' => 'auto_increment_timestamp_withFK_ibfk_1',
                        ),
                    10 =>
                        array (
                            'key' => 'KBC.foreignKeyRefSchema',
                            'value' => 'test',
                        ),
                    11 =>
                        array (
                            'key' => 'KBC.foreignKeyRefTable',
                            'value' => 'auto_increment_timestamp',
                        ),
                    12 =>
                        array (
                            'key' => 'KBC.foreignKeyRefColumn',
                            'value' => '_weird-i-d',
                        ),
                ),
        );
        $this->assertEquals($expectedColumnMetadata, $outputManifest['column_metadata']);
    }

    public function testSchemaNotEqualToDatabase(): void
    {
        $this->createTextTable(
            new CsvFile($this->dataDir . '/mysql/sales.csv'),
            "ext_sales",
            "temp_schema"
        );

        $config = $this->getConfig();

        $config['parameters']['tables'][2]['table'] = ['schema' => 'temp_schema', 'tableName' => 'ext_sales'];
        unset($config['parameters']['tables'][0]);
        unset($config['parameters']['tables'][1]);

        try {
            $app = $this->createApplication($config);
            $app->run();
            $this->fail('table schema and database mismatch');
        } catch (\Keboola\DbExtractor\Exception\UserException $e) {
            $this->assertStringStartsWith("Invalid Configuration", $e->getMessage());
        }
    }

    public function testThousandsOfTables(): void
    {
        $this->markTestSkipped("No need to run this test every time.");
        $csv1 = new CsvFile($this->dataDir . '/mysql/sales.csv');

        for ($i = 0; $i < 3500; $i++) {
            $this->createTextTable($csv1, "sales_" . $i);
        }

        $config = $this->getConfig();
        $config['action'] = 'getTables';
        $app = $this->createApplication($config);

        $result = $app->run();
        echo "\nThere are " . count($result['tables']) . " tables\n";
    }

    public function testWeirdColumnNames(): void
    {
        $config = $this->getIncrementalFetchingConfig();
        $this->createAutoIncrementAndTimestampTable();

        $result = ($this->createApplication($config))->run();

        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2,
            ],
            $result['imported']
        );
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported']['outputTable'] . '.csv.manifest';
        $manifest = json_decode(file_get_contents($outputManifestFile), true);
        $expectedColumns = ['weird_I_d', 'weird_Name', 'timestamp'];
        $this->assertEquals($expectedColumns, $manifest['columns']);
        $this->assertEquals(['weird_I_d'], $manifest['primary_key']);
    }

    public function testIncrementalFetchingByTimestamp(): void
    {
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingColumn'] = 'timestamp';
        $this->createAutoIncrementAndTimestampTable();

        $result = ($this->createApplication($config))->run();

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
        $emptyResult = ($this->createApplication($config, $result['state']))->run();
        $this->assertEquals(0, $emptyResult['imported']['rows']);

        sleep(2);
        //now add a couple rows and run it again.
        $this->pdo->exec('INSERT INTO auto_increment_timestamp (`weird-Name`) VALUES (\'charles\'), (\'william\')');

        $newResult = ($this->createApplication($config, $result['state']))->run();

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertGreaterThan(
            $result['state']['lastFetchedRow'],
            $newResult['state']['lastFetchedRow']
        );
        $this->assertEquals(2, $newResult['imported']['rows']);
    }

    public function testIncrementalFetchingByDatetime(): void
    {
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingColumn'] = 'datetime';
        $config['parameters']['table']['tableName'] = 'auto_increment_timestamp_withFK';
        $config['parameters']['outputTable'] = 'in.c-main.auto-increment-timestamp-with-fk';
        $this->createAutoIncrementAndTimestampTable();
        $this->createAutoIncrementAndTimestampTableWithFK();

        $result = ($this->createApplication($config))->run();

        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp-with-fk',
                'rows' => 1,
            ],
            $result['imported']
        );

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertNotEmpty($result['state']['lastFetchedRow']);

        sleep(2);
        // the next fetch should be empty
        $emptyResult = ($this->createApplication($config, $result['state']))->run();
        $this->assertEquals(0, $emptyResult['imported']['rows']);

        sleep(2);
        //now add a couple rows and run it again.
        $this->pdo->exec('INSERT INTO auto_increment_timestamp_withFK (`random_name`) VALUES (\'charles\'), (\'william\')');

        $newResult = ($this->createApplication($config, $result['state']))->run();

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertGreaterThan(
            $result['state']['lastFetchedRow'],
            $newResult['state']['lastFetchedRow']
        );
        $this->assertEquals(2, $newResult['imported']['rows']);
    }

    public function testIncrementalFetchingByAutoIncrement(): void
    {
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingColumn'] = '_weird-I-d';
        $this->createAutoIncrementAndTimestampTable();

        $result = ($this->createApplication($config))->run();

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
        $this->assertEquals(2, $result['state']['lastFetchedRow']);

        sleep(2);
        // the next fetch should be empty
        $emptyResult = ($this->createApplication($config, $result['state']))->run();
        $this->assertEquals(0, $emptyResult['imported']['rows']);

        sleep(2);
        //now add a couple rows and run it again.
        $this->pdo->exec('INSERT INTO auto_increment_timestamp (`weird-Name`) VALUES (\'charles\'), (\'william\')');

        $newResult = ($this->createApplication($config, $result['state']))->run();

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertEquals(4, $newResult['state']['lastFetchedRow']);
        $this->assertEquals(2, $newResult['imported']['rows']);
    }

    public function testIncrementalFetchingLimit(): void
    {
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingLimit'] = 1;
        $this->createAutoIncrementAndTimestampTable();

        $result = ($this->createApplication($config))->run();

        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 1,
            ],
            $result['imported']
        );

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(1, $result['state']['lastFetchedRow']);

        sleep(2);
        // the next fetch should contain the second row
        $result = ($this->createApplication($config, $result['state']))->run();
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 1,
            ],
            $result['imported']
        );

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(2, $result['state']['lastFetchedRow']);
    }


    public function testIncrementalFetchingInvalidColumns(): void
    {
        $this->createAutoIncrementAndTimestampTable();
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingColumn'] = 'fakeCol'; // column does not exist

        try {
            $result = ($this->createApplication($config))->run();
            $this->fail('specified autoIncrement column does not exist, should fail.');
        } catch (UserException $e) {
            $this->assertStringStartsWith("Column [fakeCol]", $e->getMessage());
        }

        // column exists but is not auto-increment nor updating timestamp so should fail
        $config['parameters']['incrementalFetchingColumn'] = 'weird-Name';
        try {
            $result = ($this->createApplication($config))->run();
            $this->fail('specified column is not auto increment nor timestamp, should fail.');
        } catch (UserException $e) {
            $this->assertStringStartsWith("Column [weird-Name] specified for incremental fetching", $e->getMessage());
        }
    }

    public function testIncrementalFetchingInvalidConfig(): void
    {
        $this->createAutoIncrementAndTimestampTable();
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['query'] = 'SELECT * FROM auto_increment_timestamp';
        unset($config['parameters']['table']);

        try {
            $result = ($this->createApplication($config))->run();
            $this->fail('cannot use incremental fetching with advanced query, should fail.');
        } catch (UserException $e) {
            $this->assertStringStartsWith("Invalid Configuration", $e->getMessage());
        }
    }

    public function testRunWithNetworkCompression(): void
    {
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['db']['networkCompression'] = true;
        $result = ($this->createApplication($config))->run();
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
        $this->assertEquals(2, $result['state']['lastFetchedRow']);
    }

    private function getIncrementalFetchingConfig(): array
    {
        $config = $this->getConfigRow(self::DRIVER);
        unset($config['parameters']['query']);
        $config['parameters']['table'] = [
            'tableName' => 'auto_increment_timestamp',
            'schema' => 'test',
        ];
        $config['parameters']['incremental'] = true;
        $config['parameters']['name'] = 'auto-increment-timestamp';
        $config['parameters']['outputTable'] = 'in.c-main.auto-increment-timestamp';
        $config['parameters']['primaryKey'] = ['_weird-I-d'];
        $config['parameters']['incrementalFetchingColumn'] = '_weird-I-d';
        return $config;
    }
}
