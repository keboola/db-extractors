<?php
/**
 * @package ex-db-mysql
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\DbExtractor;

use Keboola\Csv\CsvFile;

class MySQLTest extends AbstractMySQLTest
{
	public function setUp()
	{
		if (!defined('APP_NAME')) {
			define('APP_NAME', 'ex-db-mysql');
		}

		$options = [
			\PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
			\PDO::MYSQL_ATTR_LOCAL_INFILE => true
		];

		$config = $this->getConfig('mysql');
		$dbConfig = $config['parameters']['db'];

		$dsn = sprintf(
			"mysql:host=%s;port=%s;dbname=%s;charset=utf8",
			$dbConfig['host'],
			$dbConfig['port'],
			$dbConfig['database']
		);

		$this->pdo = new \PDO($dsn, $dbConfig['user'], $dbConfig['password'], $options);
		$this->pdo->setAttribute(\PDO::MYSQL_ATTR_LOCAL_INFILE, true);
		$this->pdo->exec("SET NAMES utf8;");
	}

	public function testCredentials()
	{
		$config = $this->getConfig('mysql');
		$config['action'] = 'testConnection';
		unset($config['parameters']['tables']);

		$app = $this->createApplication($config);
		$result = $app->run();

		$this->assertArrayHasKey('status', $result);
		$this->assertEquals('ok', $result['status']);
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

		$this->assertEquals('ok', $result['status']);
		$this->assertFileExists($outputCsvFile);
		$this->assertFileExists($this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest');
		$this->assertFileEquals((string) $csv1, $outputCsvFile);


		$outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][1] . '.csv';

		$this->assertEquals('ok', $result['status']);
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
		$this->assertEquals('ok', $result['status']);
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

		$this->assertEquals('ok', $result['status']);
		$this->assertFileExists($outputCsvFile);
		$this->assertFileExists($this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest');
		$this->assertFileEquals((string) $csv1, $outputCsvFile);


		$outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][1] . '.csv';

		$this->assertEquals('ok', $result['status']);
		$this->assertFileExists($outputCsvFile);
		$this->assertFileExists($this->dataDir . '/out/tables/' . $result['imported'][1] . '.csv.manifest');
		$this->assertFileEquals((string) $csv2, $outputCsvFile);
	}
}
