<?php

/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 10/12/15
 * Time: 14:25
 */

namespace Keboola\DbExtractor;

use Symfony\Component\Yaml\Yaml;

class RedshiftTest extends AbstractRedshiftTest
{
    private function runApp(Application $app)
    {
        $result = $app->run();
        $expectedCsvFile = $this->dataDir .  "/in/tables/escaping.csv";
        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv';
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest';
        $manifest = Yaml::parse(file_get_contents($outputManifestFile));

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($outputManifestFile);;
        $this->assertEquals(file_get_contents($expectedCsvFile), file_get_contents($outputCsvFile));
        $this->assertEquals('in.c-main.escaping', $manifest['destination']);
        $this->assertEquals(true, $manifest['incremental']);
        $this->assertEquals('col3', $manifest['primary_key'][0]);
    }

    public function testRun()
    {
        $this->runApp(new Application($this->getConfig()));
    }

    public function testRunWithSSH()
    {
        $config = $this->getConfig();
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getEnv('redshift', 'DB_SSH_KEY_PRIVATE'),
                'public' => $this->getEnv('redshift', 'DB_SSH_KEY_PUBLIC')
            ],
            'user' => 'root',
            'sshHost' => 'sshproxy',
            'localPort' => '33306',
            'remoteHost' => $this->getEnv('redshift', 'DB_HOST'),
            'remotePort' => $this->getEnv('redshift', 'DB_PORT')
        ];
        $this->runApp(new Application($config));
    }

    public function testTestConnection()
    {
        $config = $this->getConfig();
        $config['action'] = 'testConnection';

        $app = new Application($config);
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
                '#private' => $this->getEnv('redshift', 'DB_SSH_KEY_PRIVATE'),
                'public' => $this->getEnv('redshift', 'DB_SSH_KEY_PUBLIC')
            ],
            'user' => 'root',
            'sshHost' => 'sshproxy',
            'localPort' => '33307',
            'remoteHost' => $this->getEnv('redshift', 'DB_HOST'),
            'remotePort' => $this->getEnv('redshift', 'DB_PORT')
        ];

        $app = new Application($config);
        $result = $app->run();
        $this->assertEquals('success', $result['status']);
    }
    public function testGetTables()
    {
        $config = $this->getConfig();
        $config['action'] = 'getTables';
        $app = new Application($config);
        $result = $app->run();
        $this->assertArrayHasKey('status', $result);
        $this->assertArrayHasKey('tables', $result);
        $this->assertCount(1, $result['tables']);

        $expectedData = array (
            0 =>
                array (
                    'name' => 'escaping',
                    'schema' => 'testing',
                    'type' => 'BASE TABLE',
                    'catalog' => 'travis_ex_db_tests',
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

        $config['parameters']['tables'][0]['columns'] = ["col1","col2","col3"];
        $config['parameters']['tables'][0]['table'] = 'escaping';
        $config['parameters']['tables'][0]['query'] = "SELECT col1, col2, col3 FROM escaping";
        // use just 1 table
        unset($config['parameters']['tables'][1]);

        $app = new Application($config);

        $result = $app->run();

        $outputManifest = Yaml::parse(
            file_get_contents($this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest')
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
                    'value' => 'testing',
                ),
            2 =>
                array (
                    'key' => 'KBC.type',
                    'value' => 'BASE TABLE',
                ),
            3 =>
                array (
                    'key' => 'KBC.catalog',
                    'value' => 'travis_ex_db_tests',
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
                      'key' => 'KBC.primaryKey',
                      'value' => true,
                  ),
              6 =>
                  array (
                      'key' => 'KBC.uniqueKey',
                      'value' => false,
                  ),
              7 =>
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
                      'key' => 'KBC.primaryKey',
                      'value' => true,
                  ),
              6 =>
                  array (
                      'key' => 'KBC.uniqueKey',
                      'value' => false,
                  ),
              7 =>
                  array (
                      'key' => 'KBC.ordinalPosition',
                      'value' => 2,
                  ),
          ),
          'col3' =>
          array (
              0 =>
                  array (
                      'key' => 'KBC.datatype.type',
                      'value' => 'character varying',
                  ),
              1 =>
                  array (
                      'key' => 'KBC.datatype.nullable',
                      'value' => true,
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
                      'key' => 'KBC.primaryKey',
                      'value' => false,
                  ),
              5 =>
                  array (
                      'key' => 'KBC.uniqueKey',
                      'value' => false,
                  ),
              6 =>
                  array (
                      'key' => 'KBC.ordinalPosition',
                      'value' => 3,
                  ),
          ),
        );
        $this->assertEquals($expectedColumnMetadata, $outputManifest['column_metadata']);
    }
}
