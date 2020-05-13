<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Exception\UserException;
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

    public function testRunMain(): void
    {
        $config = $this->getConfig();
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
        $config['parameters']['tables'][0]['query'] = 'SELECT * FROM test.sales';
        $config['parameters']['tables'][1]['query'] = 'SELECT * FROM test.escaping';

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
                '#private' => $this->getPrivateKey(),
                'public' => $this->getPublicKey(),
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
                '#private' => $this->getPrivateKey(),
                'public' => $this->getPublicKey(),
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
        $this->expectException(UserException::class);
        $config = $this->getConfig('mysql');

        $config['parameters']['db']['host'] = 'nonexistinghost';
        $app = $this->createApplication($config);

        $app->run();
    }

    public function testEmoji(): void
    {
        $this->createTextTable(
            new CsvFile($this->dataDir . '/mysql/emoji.csv'),
            'emoji',
            'test'
        );

        $config = $this->getConfigRow();
        unset($config['parameters']['primaryKey']);
        $config['parameters'] = array_merge(
            $config['parameters'],
            [
                'query' => 'SELECT * FROM emoji',
                'outputTable' => 'in.c-main.emoji',
            ]
        );

        $app = $this->createApplication($config);

        $result = $app->run();

        $expectedFile = $this->dataDir . '/mysql/emoji.csv';
        $outputFile = $this->dataDir . '/out/tables/in.c-main.emoji.csv';

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);

        $this->assertEquals(58, $result['imported']['rows']);

        $this->assertFileEquals($expectedFile, $outputFile);
    }

    public function testGetTables(): void
    {
        $this->createAutoIncrementAndTimestampTable();

        // add a table to a different schema (should not be fetched)
        $this->createTextTable(
            new CsvFile($this->dataDir . '/mysql/sales.csv'),
            'ext_sales',
            'temp_schema'
        );

        $config = $this->getConfig();
        $config['action'] = 'getTables';
        $app = $this->createApplication($config);

        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertArrayHasKey('tables', $result);

        $this->assertEquals('success', $result['status']);
        $this->assertCount(4, $result['tables']);

        $expectedData = array (
            0 =>
                array (
                    'name' => 'auto_increment_timestamp',
                    'schema' => 'test',
                    'type' => 'BASE TABLE',
                    'rowCount' => 2,
                    'columns' => $this->expectedTableColumns('test', 'auto_increment_timestamp'),
                    'description' => 'This is a table comment',
                ),
            1 => [
                'name' => 'emoji',
                'schema' => 'test',
                'type' => 'BASE TABLE',
                'rowCount' => 58,
                'columns' => $this->expectedTableColumns('test', 'emoji'),
            ],
            2 =>
                array (
                    'name' => 'escaping',
                    'schema' => 'test',
                    'type' => 'BASE TABLE',
                    'rowCount' => 7,
                    'columns' => $this->expectedTableColumns('test', 'escaping'),
                ),
            3 =>
                array (
                    'name' => 'sales',
                    'schema' => 'test',
                    'type' => 'BASE TABLE',
                    'rowCount' => 100,
                    'columns' => $this->expectedTableColumns('test', 'sales'),
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
            'ext_sales',
            'temp_schema'
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
                    'rowCount' => 100,
                    'columns' => $this->expectedTableColumns('temp_schema', 'ext_sales'),
                ),
            1 =>
                array (
                    'name' => 'auto_increment_timestamp',
                    'schema' => 'test',
                    'type' => 'BASE TABLE',
                    'rowCount' => 2,
                    'columns' => $this->expectedTableColumns('test', 'auto_increment_timestamp'),
                    'description' => 'This is a table comment'
                ),
            2 =>[
                'name' => 'emoji',
                'schema' => 'test',
                'type' => 'BASE TABLE',
                'rowCount' => 58,
                'columns' => $this->expectedTableColumns('test', 'emoji'),
            ],
            3 =>
                array (
                    'name' => 'escaping',
                    'schema' => 'test',
                    'type' => 'BASE TABLE',
                    'rowCount' => 7,
                    'columns' => $this->expectedTableColumns('test', 'escaping'),
                ),
            4 =>
                array (
                    'name' => 'sales',
                    'schema' => 'test',
                    'type' => 'BASE TABLE',
                    'rowCount' => 100,
                    'columns' => $this->expectedTableColumns('test', 'sales'),
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
            (string) file_get_contents($this->dataDir . '/out/tables/' . $sanitizedTable . '.csv.manifest'),
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
                        array(
                            'key' => 'KBC.uniqueKey',
                            'value' => false,
                        ),
                    8 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => 1,
                        ),
                    9 =>
                        array (
                            'key' => 'KBC.description',
                            'value' => 'This is a weird ID',
                        ),
                    10 =>
                        array (
                            'key' => 'KBC.autoIncrement',
                            'value' => true,
                        ),
                    11 =>
                        array(
                            'key' => 'KBC.autoIncrementValue',
                            'value' => 2,
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
                        array(
                            'key' => 'KBC.primaryKey',
                            'value' => false,
                        ),
                    8 =>
                        array (
                            'key' => 'KBC.uniqueKey',
                            'value' => false,
                        ),
                    9 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => '2',
                        ),
                    10 =>
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
                            'key' => 'KBC.uniqueKey',
                            'value' => false,
                        ),
                    8 =>
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
                            'key' => 'KBC.uniqueKey',
                            'value' => false,
                        ),
                    8 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => '4',
                        ),
                    9 =>
                        array (
                            'key' => 'KBC.description',
                            'value' => 'This is a foreign key',
                        ),
                    10 =>
                        array (
                            'key' => 'KBC.foreignKey',
                            'value' => true,
                        ),
                    11 => array(
                        'key' => 'KBC.foreignKeyName',
                        'value' => 'auto_increment_timestamp_withFK_ibfk_1',
                    ),
                    12 => array(
                        'key' => 'KBC.foreignKeyRefSchema',
                        'value' => 'test',
                    ),
                    13 => array(
                        'key' => 'KBC.foreignKeyRefTable',
                        'value' => 'auto_increment_timestamp',

                    ),
                    14 => array(
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
            'ext_sales',
            'temp_schema'
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
            $this->assertStringStartsWith('Invalid Configuration', $e->getMessage());
        }
    }

    public function testThousandsOfTables(): void
    {
        $this->markTestSkipped('No need to run this test every time.');
        $csv1 = new CsvFile($this->dataDir . '/mysql/sales.csv');

        for ($i = 0; $i < 3500; $i++) {
            $this->createTextTable($csv1, 'sales_' . $i);
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
        $manifest = json_decode((string) file_get_contents($outputManifestFile), true);
        $expectedColumns = ['weird_I_d', 'weird_Name', 'timestamp', 'datetime', 'intColumn', 'decimalColumn'];
        $this->assertEquals($expectedColumns, $manifest['columns']);
        $this->assertEquals(['weird_I_d'], $manifest['primary_key']);
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

    public function testDBSchemaMismatchConfigRowWithNoName(): void
    {
        $config = $this->getConfigRow(self::DRIVER);
        // select a table from a different schema
        unset($config['parameters']['query']);
        $config['parameters']['table'] = [
            'tableName' => 'ext_sales',
            'schema' => 'temp_schema',
        ];
        try {
            ($this->createApplication($config))->run();
            $this->fail('Should throw a user exception.');
        } catch (UserException $e) {
            $this->assertStringStartsWith('Invalid Configuration [ext_sales]', $e->getMessage());
        }
    }

    public function testTestIgnoringExtraKeys(): void
    {
        $configurationArray = $this->getConfigRow(self::DRIVER);
        $configurationArray['parameters']['someExtraKey'] = 'test';
        $app = $this->createApplication($configurationArray);
        $result = $app->run();

        $this->assertEquals('success', $result['status']);
    }

    public function testIncrementalNotPresentNoResults(): void
    {
        $configurationArray = $this->getConfigRow(self::DRIVER);
        unset($configurationArray['parameters']['incremental']);
        $configurationArray['parameters']['query'] = 'SELECT * FROM sales WHERE 1 = 2;'; // no results
        $app = $this->createApplication($configurationArray);
        $result = $app->run();

        $this->assertEquals('success', $result['status']);
    }
}
