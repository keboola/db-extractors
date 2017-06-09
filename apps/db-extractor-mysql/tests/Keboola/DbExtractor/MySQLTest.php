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
			'remoteHost' => 'mysql',
			'remotePort' => '3306',
			'localPort' => '13306',
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
			'remoteHost' => 'mysql',
			'remotePort' => '3306',
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
        $config = $this->getConfig();
        $config['action'] = 'getTables';
        $app = new Application($config);

        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertArrayHasKey('tables', $result);

        $this->assertEquals('success', $result['status']);
        $this->assertCount(2, $result['tables']);
        $this->assertArrayHasKey('name', $result['tables'][0]);
        $this->assertEquals("escaping", $result['tables'][0]['name']);
        $this->assertArrayHasKey('columns', $result['tables'][0]);

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

    public function testManifestMetadata()
    {
        $config = $this->getConfig();

        $config['parameters']['tables'][0]['columns'] = ["usergender","usercity","usersentiment","zipcode"];
        $config['parameters']['tables'][0]['table'] = 'sales';
        $config['parameters']['tables'][0]['query'] = "SELECT usergender, usercity, usersentiment, zipcode FROM sales";
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
                    $this->assertEquals('sales', $metadata['value']);
                    break;
                case 'KBC.schema':
                    $this->assertEquals('test', $metadata['value']);
                    break;
                case 'KBC.type':
                    $this->assertEquals('BASE TABLE', $metadata['value']);
                    break;
                case 'KBC.rowCount':
                    $this->assertEquals(100, $metadata['value']);
                    break;
                default:
                    $this->fail('Unknown table metadata key: ' . $metadata['key']);
            }
        }
        $this->assertArrayHasKey('column_metadata', $outputManifest);
        $this->assertCount(4, $outputManifest['column_metadata']);
        foreach ($outputManifest['column_metadata']['usergender'] as $metadata) {
            $this->assertArrayHasKey('key', $metadata);
            $this->assertArrayHasKey('value', $metadata);
            switch ($metadata['key']) {
                case 'KBC.datatype.type':
                    $this->assertEquals('text', $metadata['value']);
                    break;
                case 'KBC.datatype.basetype':
                    $this->assertEquals('STRING', $metadata['value']);
                    break;
                case 'KBC.datatype.nullable':
                    $this->assertTrue($metadata['value']);
                    break;
                case 'KBC.datatype.default':
                    $this->assertNull($metadata['value']);
                    break;
                case 'KBC.datatype.length':
                    $this->assertEquals('65535', $metadata['value']);
                    break;
                case 'KBC.primaryKey':
                    $this->assertFalse($metadata['value']);
                    break;
                case 'KBC.ordinalPosition':
                    $this->assertEquals(1, $metadata['value']);
                    break;
                case 'KBC.foreignKeyRefSchema':
                    $this->assertEquals('test', $metadata['value']);
                    break;
                default:
                    $this->fail("Unnexpected metadata key " . $metadata['key']);
            }
        }
    }
}
