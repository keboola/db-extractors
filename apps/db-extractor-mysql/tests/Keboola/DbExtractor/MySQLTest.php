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
    public function testCredentials()
    {
        $config = $this->getConfig();
        $config['action'] = 'testConnection';
        $config['parameters']['tables'] = [];

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    public function testCredentialsWithoutDatabase()
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

    public function testRunWithoutTables()
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
    public function testRunMain($configType)
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

    public function testRunWithoutDatabase()
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

    public function testCredentialsWithSSH()
    {
        $config = $this->getConfig();
        $config['action'] = 'testConnection';

        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getPrivateKey('mysql'),
                'public' => $this->getEnv('mysql', 'DB_SSH_KEY_PUBLIC')
            ],
            'user' => 'root',
            'sshHost' => 'sshproxy',
            'sshPort' => '22',
            'remoteHost' => 'mysql',
            'remotePort' => '3306',
            'localPort' => '23305',
        ];

        $config['parameters']['tables'] = [];

        $app = $this->createApplication($config);

        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    public function testRunWithSSH()
    {
        $config = $this->getConfig();
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getPrivateKey('mysql'),
                'public' => $this->getEnv('mysql', 'DB_SSH_KEY_PUBLIC')
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

    public function testUserException()
    {
        $this->setExpectedException('Keboola\DbExtractor\Exception\UserException');

        $config = $this->getConfig('mysql');

        $config['parameters']['db']['host'] = 'nonexistinghost';
        $app = $this->createApplication($config);

        $app->run();
    }

    public function testGetTables()
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

    public function testGetTablesNoDatabase()
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

    public function testManifestMetadata()
    {
        $config = $this->getConfig();

        // use just the last table from the config
        unset($config['parameters']['tables'][0]);
        unset($config['parameters']['tables'][1]);

        $app = $this->createApplication($config);

        $result = $app->run();

        $sanitizedTable = Utils\Strings::webalize($result['imported'][0]['outputTable'], '._');
        $outputManifest = Yaml::parse(
            file_get_contents($this->dataDir . '/out/tables/' . $sanitizedTable . '.csv.manifest')
        );

        $this->assertArrayHasKey('destination', $outputManifest);
        $this->assertArrayHasKey('incremental', $outputManifest);
        $this->assertArrayHasKey('metadata', $outputManifest);
        $expectedMetadata = [
            'KBC.name' => 'sales',
            'KBC.schema' => 'test',
            'KBC.type' => 'BASE TABLE',
            'KBC.rowCount' => 100
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

        $expectedColumnMetadata = [
            'KBC.sourceName' => 'usergender',
            'KBC.sanitizedName' => 'usergender',
            'KBC.datatype.type' => 'text',
            'KBC.datatype.basetype' => 'STRING',
            'KBC.datatype.nullable' => true,
            'KBC.datatype.length' => '65535',
            'KBC.primaryKey' => false,
            'KBC.ordinalPosition' => '1'
        ];
        $columnMetadata = [];
        foreach ($outputManifest['column_metadata']['usergender'] as $metadata) {
            $this->assertArrayHasKey('key', $metadata);
            $this->assertArrayHasKey('value', $metadata);
            $columnMetadata[$metadata['key']] = $metadata['value'];
        }
        $this->assertEquals($expectedColumnMetadata, $columnMetadata);
    }

    public function testSchemaNotEqualToDatabase()
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

    public function testThousandsOfTables()
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

    public function testWeirdColumnNames()
    {
        $config = $this->getIncrementalFetchingConfig();
        $this->createAutoIncrementAndTimestampTable();

        $result = (new MySQLApplication($config))->run();

        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2
            ],
            $result['imported']
        );
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported']['outputTable'] . '.csv.manifest';
        $manifest = json_decode(file_get_contents($outputManifestFile), true);
        $expectedColumns = ['weird_I_d', 'weird_Name', 'timestamp'];
        $this->assertEquals($expectedColumns, $manifest['columns']);
        $this->assertEquals(['weird_I_d'], $manifest['primary_key']);
    }

    public function testIncrementalFetchingByTimestamp()
    {
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingColumn'] = 'timestamp';
        $this->createAutoIncrementAndTimestampTable();

        $result = (new MySQLApplication($config))->run();

        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2
            ],
            $result['imported']
        );

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertNotEmpty($result['state']['lastFetchedRow']);

        sleep(2);
        // the next fetch should be empty
        $emptyResult = (new MySQLApplication($config, $result['state']))->run();
        $this->assertEquals(0, $emptyResult['imported']['rows']);

        sleep(2);
        //now add a couple rows and run it again.
        $this->pdo->exec('INSERT INTO auto_increment_timestamp (`weird-Name`) VALUES (\'charles\'), (\'william\')');

        $newResult = (new MySQLApplication($config, $result['state']))->run();

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertGreaterThan(
            $result['state']['lastFetchedRow'],
            $newResult['state']['lastFetchedRow']
        );
    }

    public function testIncrementalFetchingByAutoIncrement()
    {
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingColumn'] = '_weird-I-d';
        $this->createAutoIncrementAndTimestampTable();

        $result = (new MySQLApplication($config))->run();

        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2
            ],
            $result['imported']
        );

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(2, $result['state']['lastFetchedRow']);

        sleep(2);
        // the next fetch should be empty
        $emptyResult = (new MySQLApplication($config, $result['state']))->run();
        $this->assertEquals(0, $emptyResult['imported']['rows']);

        sleep(2);
        //now add a couple rows and run it again.
        $this->pdo->exec('INSERT INTO auto_increment_timestamp (`weird-Name`) VALUES (\'charles\'), (\'william\')');

        $newResult = (new MySQLApplication($config, $result['state']))->run();

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertEquals(4, $newResult['state']['lastFetchedRow']);
        $this->assertEquals(2, $newResult['imported']['rows']);
    }

    public function testIncrementalFetchingLimit()
    {
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingLimit'] = 1;
        $this->createAutoIncrementAndTimestampTable();

        $result = (new MySQLApplication($config))->run();

        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 1
            ],
            $result['imported']
        );

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(1, $result['state']['lastFetchedRow']);

        sleep(2);
        // the next fetch should contain the second row
        $result = (new MySQLApplication($config, $result['state']))->run();
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 1
            ],
            $result['imported']
        );

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(2, $result['state']['lastFetchedRow']);
    }


    public function testIncrementalFetchingInvalidColumns()
    {
        $this->createAutoIncrementAndTimestampTable();
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingColumn'] = 'fakeCol'; // column does not exist

        try {
            $result = (new MySQLApplication($config))->run();
            $this->fail('specified autoIncrement column does not exist, should fail.');
        } catch (UserException $e) {
            $this->assertStringStartsWith("Column [fakeCol]", $e->getMessage());
        }

        // column exists but is not auto-increment nor updating timestamp so should fail
        $config['parameters']['incrementalFetchingColumn'] = 'weird-Name';
        try {
            $result = (new MySQLApplication($config))->run();
            $this->fail('specified column is not auto increment nor timestamp, should fail.');
        } catch (UserException $e) {
            $this->assertStringStartsWith("Column [weird-Name] specified for incremental fetching", $e->getMessage());
        }
    }

    public function testIncrementalFetchingInvalidConfig()
    {
        $this->createAutoIncrementAndTimestampTable();
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['query'] = 'SELECT * FROM auto_increment_timestamp';
        unset($config['parameters']['table']);

        try {
            $result = (new MySQLApplication($config))->run();
            $this->fail('cannot use incremental fetching with advanced query, should fail.');
        } catch (UserException $e) {
            $this->assertStringStartsWith("Invalid Configuration", $e->getMessage());
        }
    }

    public function testRunWithNetworkCompression()
    {
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['db']['networkCompression'] = true;
        $result = ($this->createApplication($config))->run();
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2
            ],
            $result['imported']
        );

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(2, $result['state']['lastFetchedRow']);
    }

    private function getIncrementalFetchingConfig()
    {
        $config = $this->getConfigRow(self::DRIVER);
        unset($config['parameters']['query']);
        $config['parameters']['table'] = [
            'tableName' => 'auto_increment_timestamp',
            'schema' => 'test'
        ];
        $config['parameters']['incremental'] = true;
        $config['parameters']['name'] = 'auto-increment-timestamp';
        $config['parameters']['outputTable'] = 'in.c-main.auto-increment-timestamp';
        $config['parameters']['primaryKey'] = ['_weird-I-d'];
        $config['parameters']['incrementalFetchingColumn'] = '_weird-I-d';
        return $config;
    }
}
