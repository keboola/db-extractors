<?php

namespace Keboola\DbExtractor\Tests;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Application;
use Symfony\Component\Yaml\Yaml;

class RedshiftTest extends AbstractRedshiftTest
{
    private function runApp(Application $app)
    {
        $result = $app->run();
        $expectedCsvFile = $this->dataDir .  "/in/tables/escaping.csv";
        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv';
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv.manifest';
        $manifest = json_decode(file_get_contents($outputManifestFile), true);

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($outputManifestFile);
        $this->assertEquals(file_get_contents($expectedCsvFile), file_get_contents($outputCsvFile));
        $this->assertEquals('in.c-main.escaping', $manifest['destination']);
        $this->assertEquals(true, $manifest['incremental']);
        $this->assertEquals('col3', $manifest['primary_key'][0]);
    }

    public function testRunConfig()
    {
        $this->runApp($this->createApplication($this->getConfig()));
    }

    public function testRunConfigRow(): void
    {
        $app = $this->createApplication($this->getConfigRow(self::DRIVER));

        $result = $app->run();
        $expectedOutput = iterator_to_array(new CsvFile($this->dataDir .  "/in/tables/escaping.csv"));
        array_shift($expectedOutput);
        $outputArray = iterator_to_array(new CsvFile(
            $this->dataDir . '/out/tables/' . $result['imported']['outputTable'] . '.csv'
        ));
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported']['outputTable'] . '.csv.manifest';
        $manifest = json_decode(file_get_contents($outputManifestFile), true);

        $expectedColumnMetadata = array (
            'col1' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'character varying',
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
                            'value' => 256,
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.datatype.default',
                            'value' => 'a',
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.sourceName',
                            'value' => 'col1',
                        ),
                    6 =>
                        array (
                            'key' => 'KBC.primaryKey',
                            'value' => true,
                        ),
                    7 =>
                        array (
                            'key' => 'KBC.uniqueKey',
                            'value' => false,
                        ),
                    8 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => 1,
                        ),
                ),
            'col2' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'character varying',
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
                            'value' => 256,
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.datatype.default',
                            'value' => 'b',
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.sourceName',
                            'value' => 'col2',
                        ),
                    6 =>
                        array (
                            'key' => 'KBC.primaryKey',
                            'value' => true,
                        ),
                    7 =>
                        array (
                            'key' => 'KBC.uniqueKey',
                            'value' => false,
                        ),
                    8 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => 2,
                        ),
                ),
        );
        $this->assertEquals('success', $result['status']);
        $this->assertEquals(7, (int) $result['imported']['rows']);
        $this->assertEquals(ksort($expectedOutput), ksort($outputArray));
        $this->assertEquals('in.c-main.tableColumns', $manifest['destination']);
        $this->assertEquals(false, $manifest['incremental']);
        $this->assertEquals(['col1', 'col2'], $manifest['columns']);
        $this->assertEquals($expectedColumnMetadata, $manifest['column_metadata']);
    }

    public function testRunWithSSH()
    {
        $config = $this->getConfig();
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getRedshiftPrivateKey(),
                'public' => $this->getEnv('redshift', 'DB_SSH_KEY_PUBLIC')
            ],
            'user' => 'root',
            'sshHost' => 'sshproxy',
            'localPort' => '33306',
            'remoteHost' => $this->getEnv('redshift', 'DB_HOST'),
            'remotePort' => $this->getEnv('redshift', 'DB_PORT')
        ];
        $this->runApp($this->createApplication($config));
    }

    public function testRunFailure()
    {
        $config = $this->getConfig();
        $config['parameters']['tables'][] = [
            'id' => 10,
            'name' => 'bad',
            'query' => 'SELECT something FROM non_existing_table;',
            'outputTable' => 'dummy'
        ];
        try {
            $this->runApp($this->createApplication($config));
            $this->fail("Failing query must raise exception.");
        } catch (\Keboola\DbExtractor\Exception\UserException $e) {
            // test that the error message contains the query name
            $this->assertContains('[dummy]: DB query failed: SQLSTATE[42P01]:', $e->getMessage());
        }
    }

    public function testTestConnection()
    {
        $config = $this->getConfig();
        $config['action'] = 'testConnection';

        $app = $this->createApplication($config);
        $result = $app->run();
        $this->assertEquals('success', $result['status']);
    }

    public function testSSHConnection()
    {
        $config = $this->getConfig();
        $config['action'] = 'testConnection';

        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getRedshiftPrivateKey(),
                'public' => $this->getEnv('redshift', 'DB_SSH_KEY_PUBLIC')
            ],
            'user' => 'root',
            'sshHost' => 'sshproxy',
            'localPort' => '33307',
            'remoteHost' => $this->getEnv('redshift', 'DB_HOST'),
            'remotePort' => $this->getEnv('redshift', 'DB_PORT')
        ];

        $app = $this->createApplication($config);
        $result = $app->run();
        $this->assertEquals('success', $result['status']);
    }
    public function testGetTables()
    {
        $config = $this->getConfig();
        $config['action'] = 'getTables';
        $app = $this->createApplication($config);
        $result = $app->run();
        $this->assertArrayHasKey('status', $result);
        $this->assertArrayHasKey('tables', $result);

        $this->assertCount(1, $result['tables']);

        $expectedData = array (
                array (
                    'name' => 'escaping',
                    'schema' => self::TESTING_SCHEMA_NAME,
                    'type' => 'BASE TABLE',
                    'catalog' => $config['parameters']['db']['database'],
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'col1',
                                    'type' => 'character varying',
                                    'primaryKey' => true,
                                    'uniqueKey' => false,
                                    'length' => 256,
                                    'nullable' => false,
                                    'default' => 'a',
                                    'ordinalPosition' => 1,
                                ),
                            1 =>
                                array (
                                    'name' => 'col2',
                                    'type' => 'character varying',
                                    'primaryKey' => true,
                                    'uniqueKey' => false,
                                    'length' => 256,
                                    'nullable' => false,
                                    'default' => 'b',
                                    'ordinalPosition' => 2,
                                ),
                            2 =>
                                array (
                                    'name' => 'col3',
                                    'type' => 'character varying',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                    'length' => 256,
                                    'nullable' => true,
                                    'default' => NULL,
                                    'ordinalPosition' => 3,
                                ),
                        ),
                ),
        );
        $this->assertEquals($expectedData, $result['tables']);
    }

    public function testManifestMetadata()
    {
        $config = $this->getConfig();

        // use just 1 table
        unset($config['parameters']['tables'][0]);
        unset($config['parameters']['tables'][1]);

        $app = $this->createApplication($config);

        $result = $app->run();

        $outputManifest = Yaml::parse(
            file_get_contents(
                $this->dataDir . '/out/tables/' . strtolower($result['imported'][0]['outputTable']) . '.csv.manifest'
            )
        );

        $this->assertArrayHasKey('destination', $outputManifest);
        $this->assertArrayHasKey('incremental', $outputManifest);
        $this->assertArrayHasKey('metadata', $outputManifest);

        $expectedTableMetadata = array (
            0 =>
                array (
                    'key' => 'KBC.name',
                    'value' => 'escaping',
                ),
            1 =>
                array (
                    'key' => 'KBC.schema',
                    'value' => self::TESTING_SCHEMA_NAME,
                ),
            2 =>
                array (
                    'key' => 'KBC.type',
                    'value' => 'BASE TABLE',
                ),
            3 =>
                array (
                    'key' => 'KBC.catalog',
                    'value' => $config['parameters']['db']['database'],
                ),
        );

        $this->assertEquals($expectedTableMetadata, $outputManifest['metadata']);

        $expectedColumnMetadata = array (
            'col1' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'character varying',
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
                            'value' => 256,
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.datatype.default',
                            'value' => 'a',
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.sourceName',
                            'value' => 'col1',
                        ),
                    6 =>
                        array (
                            'key' => 'KBC.primaryKey',
                            'value' => true,
                        ),
                    7 =>
                        array (
                            'key' => 'KBC.uniqueKey',
                            'value' => false,
                        ),
                    8 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => 1,
                        ),
                ),
            'col2' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'character varying',
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
                            'value' => 256,
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.datatype.default',
                            'value' => 'b',
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.sourceName',
                            'value' => 'col2',
                        ),
                    6 =>
                        array (
                            'key' => 'KBC.primaryKey',
                            'value' => true,
                        ),
                    7 =>
                        array (
                            'key' => 'KBC.uniqueKey',
                            'value' => false,
                        ),
                    8 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => 2,
                        ),
                ),
        );
        $this->assertEquals($expectedColumnMetadata, $outputManifest['column_metadata']);
    }
}
