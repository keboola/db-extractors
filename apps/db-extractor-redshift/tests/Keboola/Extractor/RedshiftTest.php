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
        $this->assertArrayHasKey('name', $result['tables'][0]);
        $this->assertEquals("escaping", $result['tables'][0]['name']);
        $this->assertEquals(self::TESTING_SCHEMA_NAME, $result['tables'][0]['schema']);
        $this->assertEquals(self::TESTING_SCHEMA_NAME, $result['tables'][0]['catalog']);
        $this->assertEquals("BASE TABLE", $result['tables'][0]['type']);
        $this->assertArrayHasKey('columns', $result['tables'][0]);
        $this->assertCount(3, $result['tables'][0]['columns']);
        $this->assertArrayHasKey('name', $result['tables'][0]['columns'][0]);
        $this->assertEquals("col1", $result['tables'][0]['columns'][0]['name']);
        $this->assertArrayHasKey('type', $result['tables'][0]['columns'][0]);
        $this->assertEquals("character varying", $result['tables'][0]['columns'][0]['type']);
        $this->assertArrayHasKey('length', $result['tables'][0]['columns'][0]);
        $this->assertEquals(256, $result['tables'][0]['columns'][0]['length']);
        $this->assertArrayHasKey('nullable', $result['tables'][0]['columns'][0]);
        $this->assertFalse($result['tables'][0]['columns'][0]['nullable']);
        $this->assertArrayHasKey('default', $result['tables'][0]['columns'][0]);
        $this->assertEquals("a", $result['tables'][0]['columns'][0]['default']);
        $this->assertArrayHasKey('primaryKey', $result['tables'][0]['columns'][0]);
        $this->asserttrue($result['tables'][0]['columns'][0]['primaryKey']);
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
        foreach ($outputManifest['metadata'] as $i => $metadata) {
            $this->assertArrayHasKey('key', $metadata);
            $this->assertArrayHasKey('value', $metadata);
            switch ($metadata['key']) {
                case 'KBC.name':
                    $this->assertEquals('escaping', $metadata['value']);
                    break;
                case 'KBC.schema':
                    $this->assertEquals(self::TESTING_SCHEMA_NAME, $metadata['value']);
                    break;
                case 'KBC.catalog':
                    $this->assertEquals(self::TESTING_SCHEMA_NAME, $metadata['value']);
                    break;
                case 'KBC.type':
                    $this->assertEquals('BASE TABLE', $metadata['value']);
                    break;
                default:
                    $this->fail('Unknown table metadata key: ' . $metadata['key']);
            }
        }
        $this->assertArrayHasKey('column_metadata', $outputManifest);
        $this->assertCount(3, $outputManifest['column_metadata']);
        foreach ($outputManifest['column_metadata']['col1'] as $metadata) {
            $this->assertArrayHasKey('key', $metadata);
            $this->assertArrayHasKey('value', $metadata);
            switch ($metadata['key']) {
                case 'KBC.datatype.type':
                    $this->assertEquals('character varying', $metadata['value']);
                    break;
                case 'KBC.datatype.basetype':
                    $this->assertEquals('STRING', $metadata['value']);
                    break;
                case 'KBC.datatype.nullable':
                    $this->assertFalse($metadata['value']);
                    break;
                case 'KBC.datatype.default':
                    $this->assertEquals("a", $metadata['value']);
                    break;
                case 'KBC.datatype.length':
                    $this->assertEquals('256', $metadata['value']);
                    break;
                case 'KBC.primaryKey':
                    $this->assertTrue($metadata['value']);
                    break;
                case 'KBC.ordinalPosition':
                    $this->assertEquals(1, $metadata['value']);
                    break;
                case 'KBC.uniqueKey':
                    $this->assertFalse($metadata['value']);
                    break;
                default:
                    $this->fail('Unknown table metadata key: ' . $metadata['key']);
            }
        }
    }
}
