<?php
/**
 * @package ex-db-mysql
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\DbExtractor;

use Keboola\Csv\CsvFile;

class MySQLSSLTest extends AbstractMySQLTest
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

		$options[\PDO::MYSQL_ATTR_SSL_KEY] = realpath($this->dataDir . '/mysql/ssl/client-key.pem');
		$options[\PDO::MYSQL_ATTR_SSL_CERT] = realpath($this->dataDir . '/mysql/ssl/client-cert.pem');
		$options[\PDO::MYSQL_ATTR_SSL_CA] = realpath($this->dataDir . '/mysql/ssl/ca.pem');

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

	public function testSSLEnabled()
	{
		$status = $this->pdo->query("SHOW STATUS LIKE 'Ssl_cipher';")->fetch(\PDO::FETCH_ASSOC);

		$this->assertArrayHasKey('Value', $status);
		$this->assertNotEmpty($status['Value']);
	}

	public function testCredentials()
	{
		$config = $this->getConfig('mysql');
		$config['action'] = 'testConnection';

		$config['parameters']['db']['ssl'] = [
			'enabled' => true,
			'ca' => file_get_contents($this->dataDir . '/mysql/ssl/ca.pem'),
			'cert' => file_get_contents($this->dataDir . '/mysql/ssl/client-cert.pem'),
			'key' => file_get_contents($this->dataDir . '/mysql/ssl/client-key.pem'),
//			'cipher' => '',
		];

		unset($config['parameters']['tables']);

		$app = $this->createApplication($config);
		$result = $app->run();

		$this->assertArrayHasKey('status', $result);
		$this->assertEquals('ok', $result['status']);
	}

	public function testRun()
	{
		$config = $this->getConfig('mysql');

		$config['parameters']['db']['ssl'] = [
			'enabled' => true,
			'ca' => file_get_contents($this->dataDir . '/mysql/ssl/ca.pem'),
			'cert' => file_get_contents($this->dataDir . '/mysql/ssl/client-cert.pem'),
			'key' => file_get_contents($this->dataDir . '/mysql/ssl/client-key.pem'),
//			'cipher' => '',
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
