<?php
/**
 * @package ex-db-mysql
 * @author Erik Zigo <erik.zigo@keboola.com>
 */

namespace Keboola\DbExtractor;

use Keboola\Csv\CsvFile;
use Symfony\Component\Yaml\Yaml;

class MySQLTest extends AbstractMySQLTest
{
    public function testCredentials()
    {
        $config = $this->getConfig('mysql');
        $config['action'] = 'testConnection';
        unset($config['parameters']['tables']);

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    public function testCredentialsWithoutDatabase()
    {
        $config = $this->getConfig('mysql');
        $config['action'] = 'testConnection';
        unset($config['parameters']['tables']);
        unset($config['parameters']['db']['database']);

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    public function testRunWithoutTables()
    {
        $config = $this->getConfig('mysql');

        unset($config['parameters']['tables']);

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    public function testRun()
    {
        $config = $this->getConfig('mysql');
        $app = $this->createApplication($config);

        $csv1 = new CsvFile($this->dataDir . '/mysql/sales.csv');
        $this->createTextTable($csv1);

        $csv2 = new CsvFile($this->dataDir . '/mysql/escaping.csv');
        $this->createTextTable($csv2);

        $result = $app->run();

        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv';

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest');
        $this->assertFileEquals((string) $csv1, $outputCsvFile);


        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][1] . '.csv';

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($this->dataDir . '/out/tables/' . $result['imported'][1] . '.csv.manifest');
        $this->assertFileEquals((string) $csv2, $outputCsvFile);
    }

    public function testRunWithoutDatabase()
    {
        $config = $this->getConfig('mysql');
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
        $config = $this->getConfig('mysql');
        $config['action'] = 'testConnection';

        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getEnv('mysql', 'DB_SSH_KEY_PRIVATE'),
                'public' => $this->getEnv('mysql', 'DB_SSH_KEY_PUBLIC')
            ],
            'user' => 'root',
            'sshHost' => 'sshproxy',
            'sshPort' => '22',
            'remoteHost' => 'mysql',
            'remotePort' => '3306',
            'localPort' => '23305',
        ];

        unset($config['parameters']['tables']);

        $app = $this->createApplication($config);

        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    public function testRunWithSSH()
    {
        $config = $this->getConfig('mysql');
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getEnv('mysql', 'DB_SSH_KEY_PRIVATE'),
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

        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv';

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest');
        $this->assertFileEquals((string) $csv1, $outputCsvFile);

        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][1] . '.csv';

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($this->dataDir . '/out/tables/' . $result['imported'][1] . '.csv.manifest');
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
        // add a table to a different schema (should not be fetched)
        $this->createTextTable(
            new CsvFile($this->dataDir . '/mysql/sales.csv'),
            "ext_sales",
            "temp_schema"
        );

        $config = $this->getConfig('mysql');
        $config['action'] = 'getTables';
        $app = $this->createApplication($config);

        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertArrayHasKey('tables', $result);

        $this->assertEquals('success', $result['status']);
        $this->assertCount(2, $result['tables']);

        $expectedData = [
            [
                "name" => "escaping",
                "schema" => "test",
                "type" => "BASE TABLE",
                "rowCount" => '7',
                "columns" => [
                    [
                        "name" => "col1",
                        "type" => "text",
                        "primaryKey" => false,
                        "length" => "65535",
                        "nullable" => true,
                        "default" => null,
                        "ordinalPosition" => "1"
                    ], [
                        "name" => "col2",
                        "type" => "text",
                        "primaryKey" => false,
                        "length" => "65535",
                        "nullable" => true,
                        "default" => null,
                        "ordinalPosition" => "2"
                    ]
                ]
            ], [
                "name" => "sales",
                "schema" => "test",
                "type" => "BASE TABLE",
                "rowCount" => "100",
                "columns" => [
                    [
                        "name" => "usergender",
                        "type" => "text",
                        "primaryKey" => false,
                        "length" => "65535",
                        "nullable" => true,
                        "default" => null,
                        "ordinalPosition" => "1"
                    ], [
                        "name" => "usercity",
                        "type" => "text",
                        "primaryKey" => false,
                        "length" => "65535",
                        "nullable" => true,
                        "default" => null,
                        "ordinalPosition" => "2"
                    ], [
                        "name" => "usersentiment",
                        "type" => "text",
                        "primaryKey" => false,
                        "length" => "65535",
                        "nullable" => true,
                        "default" => null,
                        "ordinalPosition" => "3"
                    ], [
                        "name" => "zipcode",
                        "type" => "text",
                        "primaryKey" => false,
                        "length" => "65535",
                        "nullable" => true,
                        "default" => null,
                        "ordinalPosition" => "4"
                    ], [
                        "name" => "sku",
                        "type" => "text",
                        "primaryKey" => false,
                        "length" => "65535",
                        "nullable" => true,
                        "default" => null,
                        "ordinalPosition" => "5"
                    ], [
                        "name" => "createdat",
                        "type" => "text",
                        "primaryKey" => false,
                        "length" => "65535",
                        "nullable" => true,
                        "default" => null,
                        "ordinalPosition" => "6"
                    ], [
                        "name" => "category",
                        "type" => "text",
                        "primaryKey" => false,
                        "length" => "65535",
                        "nullable" => true,
                        "default" => null,
                        "ordinalPosition" => "7"
                    ], [
                        "name" => "price",
                        "type" => "text",
                        "primaryKey" => false,
                        "length" => "65535",
                        "nullable" => true,
                        "default" => null,
                        "ordinalPosition" => "8"
                    ], [
                        "name" => "county",
                        "type" => "text",
                        "primaryKey" => false,
                        "length" => "65535",
                        "nullable" => true,
                        "default" => null,
                        "ordinalPosition" => "9"
                    ], [
                        "name" => "countycode",
                        "type" => "text",
                        "primaryKey" => false,
                        "length" => "65535",
                        "nullable" => true,
                        "default" => null,
                        "ordinalPosition" => "10"
                    ], [
                        "name" => "userstate",
                        "type" => "text",
                        "primaryKey" => false,
                        "length" => "65535",
                        "nullable" => true,
                        "default" => null,
                        "ordinalPosition" => "11"
                    ], [
                        "name" => "categorygroup",
                        "type" => "text",
                        "primaryKey" => false,
                        "length" => "65535",
                        "nullable" => true,
                        "default" => null,
                        "ordinalPosition" => "12"
                    ]
                ]
            ]
        ];
        $this->assertEquals($expectedData, $result['tables']);
        foreach ($result['tables'] as $table) {
            $this->assertArrayHasKey('name', $table);
            $this->assertArrayHasKey('schema', $table);
            $this->assertArrayHasKey('type', $table);
            $this->assertArrayHasKey('rowCount', $table);
            $this->assertArrayHasKey('columns', $table);
            switch ($table['name']) {
                case 'escaping':
                    $this->assertEquals('test', $table['schema']);
                    $this->assertEquals('BASE TABLE', $table['type']);
                    $this->assertEquals(7, $table['rowCount']);
                    $this->assertCount(2, $table['columns']);
                    break;
                case 'sales':
                    $this->assertEquals('test', $table['schema']);
                    $this->assertEquals('BASE TABLE', $table['type']);
                    $this->assertEquals(100, $table['rowCount']);
                    $this->assertCount(12, $table['columns']);
                    break;
            }
            foreach ($table['columns'] as $i => $column) {
                // keys
                $this->assertArrayHasKey('name', $column);
                $this->assertArrayHasKey('type', $column);
                $this->assertArrayHasKey('length', $column);
                $this->assertArrayHasKey('default', $column);
                $this->assertArrayHasKey('nullable', $column);
                $this->assertArrayHasKey('primaryKey', $column);
                $this->assertArrayHasKey('ordinalPosition', $column);
                // values
                $this->assertEquals("text", $column['type']);
                $this->assertEquals(65535, $column['length']);
                $this->assertTrue($column['nullable']);
                $this->assertNull($column['default']);
                $this->assertFalse($column['primaryKey']);
                $this->assertEquals($i + 1, $column['ordinalPosition']);
            }
        }
    }

    public function testGetTablesNoDatabase()
    {
        // add a table to a different schema
        $this->createTextTable(
            new CsvFile($this->dataDir . '/mysql/sales.csv'),
            "ext_sales",
            "temp_schema"
        );

        $config = $this->getConfig('mysql');
        unset($config['parameters']['tables']);
        unset($config['parameters']['db']['database']);
        $config['action'] = 'getTables';
        $app = $this->createApplication($config);

        $result = $app->run();

        $expectedFirstTable = array(
            'name' => 'ext_sales',
            'schema' => 'temp_schema',
            'type' => 'BASE TABLE',
            'rowCount' => '100',
            'columns' =>
                array(
                    0 =>
                        array(
                            'name' => 'usergender',
                            'type' => 'text',
                            'primaryKey' => false,
                            'length' => '65535',
                            'nullable' => true,
                            'default' => null,
                            'ordinalPosition' => '1',
                        ),
                    1 =>
                        array(
                            'name' => 'usercity',
                            'type' => 'text',
                            'primaryKey' => false,
                            'length' => '65535',
                            'nullable' => true,
                            'default' => null,
                            'ordinalPosition' => '2',
                        ),
                    2 =>
                        array(
                            'name' => 'usersentiment',
                            'type' => 'text',
                            'primaryKey' => false,
                            'length' => '65535',
                            'nullable' => true,
                            'default' => null,
                            'ordinalPosition' => '3',
                        ),
                    3 =>
                        array(
                            'name' => 'zipcode',
                            'type' => 'text',
                            'primaryKey' => false,
                            'length' => '65535',
                            'nullable' => true,
                            'default' => null,
                            'ordinalPosition' => '4',
                        ),
                    4 =>
                        array(
                            'name' => 'sku',
                            'type' => 'text',
                            'primaryKey' => false,
                            'length' => '65535',
                            'nullable' => true,
                            'default' => null,
                            'ordinalPosition' => '5',
                        ),
                    5 =>
                        array(
                            'name' => 'createdat',
                            'type' => 'text',
                            'primaryKey' => false,
                            'length' => '65535',
                            'nullable' => true,
                            'default' => null,
                            'ordinalPosition' => '6',
                        ),
                    6 =>
                        array(
                            'name' => 'category',
                            'type' => 'text',
                            'primaryKey' => false,
                            'length' => '65535',
                            'nullable' => true,
                            'default' => null,
                            'ordinalPosition' => '7',
                        ),
                    7 =>
                        array(
                            'name' => 'price',
                            'type' => 'text',
                            'primaryKey' => false,
                            'length' => '65535',
                            'nullable' => true,
                            'default' => null,
                            'ordinalPosition' => '8',
                        ),
                    8 =>
                        array(
                            'name' => 'county',
                            'type' => 'text',
                            'primaryKey' => false,
                            'length' => '65535',
                            'nullable' => true,
                            'default' => null,
                            'ordinalPosition' => '9',
                        ),
                    9 =>
                        array(
                            'name' => 'countycode',
                            'type' => 'text',
                            'primaryKey' => false,
                            'length' => '65535',
                            'nullable' => true,
                            'default' => null,
                            'ordinalPosition' => '10',
                        ),
                    10 =>
                        array(
                            'name' => 'userstate',
                            'type' => 'text',
                            'primaryKey' => false,
                            'length' => '65535',
                            'nullable' => true,
                            'default' => null,
                            'ordinalPosition' => '11',
                        ),
                    11 =>
                        array(
                            'name' => 'categorygroup',
                            'type' => 'text',
                            'primaryKey' => false,
                            'length' => '65535',
                            'nullable' => true,
                            'default' => null,
                            'ordinalPosition' => '12',
                        ),
                ),
        );
        $this->assertEquals($result['tables'][0], $expectedFirstTable);
    }

    public function testManifestMetadata()
    {
        $config = $this->getConfig('mysql');

        // use just the last table from the config
        unset($config['parameters']['tables'][0]);
        unset($config['parameters']['tables'][1]);

        $app = $this->createApplication($config);

        $result = $app->run();

        $outputManifest = Yaml::parse(
            file_get_contents($this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest')
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

    public function testThousandsOfTables()
    {
        $this->markTestSkipped("No need to run this test every time.");
        $csv1 = new CsvFile($this->dataDir . '/mysql/sales.csv');

        for ($i = 0; $i < 3500; $i++) {
            $this->createTextTable($csv1, "sales_" . $i);
        }

        $config = $this->getConfig();
        $config['action'] = 'getTables';
        $app = new Application($config);

        $result = $app->run();
        echo "\nThere are " . count($result['tables']) . " tables\n";
    }
}
