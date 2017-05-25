<?php
/**
 * @package ex-db-mysql
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\DbExtractor;

use Keboola\Csv\CsvFile;

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
        $this->assertCount(2, $result['tables'][0]['columns']);
        $this->assertArrayHasKey('name', $result['tables'][0]['columns'][0]);
        $this->assertEquals("col1", $result['tables'][0]['columns'][0]['name']);
        $this->assertArrayHasKey('type', $result['tables'][0]['columns'][0]);
        $this->assertEquals("text", $result['tables'][0]['columns'][0]['type']);
        $this->assertArrayHasKey('length', $result['tables'][0]['columns'][0]);
        $this->assertEquals(65535, $result['tables'][0]['columns'][0]['length']);
        $this->assertArrayHasKey('nullable', $result['tables'][0]['columns'][0]);
        $this->assertTrue($result['tables'][0]['columns'][0]['nullable']);
        $this->assertArrayHasKey('default', $result['tables'][0]['columns'][0]);
        $this->assertNull($result['tables'][0]['columns'][0]['default']);
        $this->assertArrayHasKey('primary', $result['tables'][0]['columns'][0]);
        $this->assertFalse($result['tables'][0]['columns'][0]['primary']);
    }
}
