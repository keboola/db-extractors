<?php
/**
 * @package ex-db-mssql
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\DbExtractor;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Test\ExtractorTest;
use Symfony\Component\Yaml\Yaml;

class MSSQLTest extends ExtractorTest
{
	/**
	 * @var \PDO
	 */
	protected $pdo;

	public function setUp()
	{
		if (!defined('APP_NAME')) {
			define('APP_NAME', 'ex-db-mssql');
		}

		$config = $this->getConfig('mssql');
		$dbConfig = $config['parameters']['db'];

		$dsn = sprintf(
			"dblib:host=%s:%d;dbname=%s;charset=UTF-8",
			$dbConfig['host'],
			$dbConfig['port'],
			$dbConfig['database']
		);

		$this->pdo = new \PDO($dsn, $dbConfig['user'], $dbConfig['password']);
		$this->pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
	}

	/**
	 * @param string $driver
	 * @return mixed
	 */
	public function getConfig($driver = 'mssql')
	{
		$config = Yaml::parse(file_get_contents($this->dataDir . '/' .$driver . '/config.yml'));
		$config['parameters']['data_dir'] = $this->dataDir;

		$config['parameters']['db']['user'] = $this->getEnv($driver, 'DB_USER', true);
		$config['parameters']['db']['password'] = $this->getEnv($driver, 'DB_PASSWORD');
		$config['parameters']['db']['host'] = $this->getEnv($driver, 'DB_HOST');
		$config['parameters']['db']['port'] = $this->getEnv($driver, 'DB_PORT');
		$config['parameters']['db']['database'] = $this->getEnv($driver, 'DB_DATABASE');

		$config['parameters']['extractor_class'] = 'MSSQL';
		return $config;
	}

	/**
	 * @param CsvFile $file
	 * @return string
	 */
	private function generateTableName(CsvFile $file)
	{
		$tableName = sprintf(
			'%s',
			$file->getBasename('.' . $file->getExtension())
		);

		return 'dbo.' . $tableName;
	}

	/**
	 * Create table from csv file with text columns
	 *
	 * @param CsvFile $file
	 */
	private function createTextTable(CsvFile $file, $primaryKey = null)
	{
		$tableName = $this->generateTableName($file);

		$this->pdo->exec(sprintf(
			'IF OBJECT_ID(\'%s\', \'U\') IS NOT NULL DROP TABLE %s',
			$tableName,
			$tableName
		));

		$sql = sprintf(
            'CREATE TABLE %s (%s)',
            $tableName,
            implode(
                ', ',
                array_map(function ($column) {
                    return $column . ' varchar(255) NULL';
                }, $file->getHeader())
            ),
            $tableName
        );

		$this->pdo->exec($sql);

        // create the primary key if supplied
        if ($primaryKey && is_array($primaryKey) && !empty($primaryKey)) {
            
            foreach ($primaryKey as $pk) {
                $sql = sprintf("ALTER TABLE %s ALTER COLUMN %s varchar(64) NOT NULL", $tableName, $pk);
                $this->pdo->exec($sql);
            }

            $sql = sprintf(
                'ALTER TABLE %s ADD PRIMARY KEY (%s)',
                $tableName,
                implode(',', $primaryKey)
            );
            $this->pdo->exec($sql);
        }

		$file->next();

		$this->pdo->beginTransaction();

		$columnsCount = count($file->current());
		$rowsPerInsert = intval((1000 / $columnsCount) - 1);


		while ($file->current() !== false) {
			$sqlInserts = "";

			for ($i=0; $i<$rowsPerInsert && $file->current() !== false; $i++) {
				$sqlInserts = "";

				$sqlInserts .= sprintf(
					"(%s),",
					implode(
						',',
						array_map(function ($data) {
							if ($data == "") return 'null';
							if (is_numeric($data)) return "'" . $data . "'";

							$nonDisplayables = array(
								'/%0[0-8bcef]/',            // url encoded 00-08, 11, 12, 14, 15
								'/%1[0-9a-f]/',             // url encoded 16-31
								'/[\x00-\x08]/',            // 00-08
								'/\x0b/',                   // 11
								'/\x0c/',                   // 12
								'/[\x0e-\x1f]/'             // 14-31
							);
							foreach ($nonDisplayables as $regex) {
								$data = preg_replace($regex, '', $data);
							}

							$data = str_replace("'", "''", $data );

							return "'" . $data . "'";
						}, $file->current())
					)
				);
				$file->next();

				$sql = sprintf('INSERT INTO %s VALUES %s',
					$tableName,
					substr($sqlInserts, 0, -1)
				);

				$this->pdo->exec($sql);
			}

//			if ($sqlInserts) {
//				$sql = sprintf('INSERT INTO %s VALUES %s',
//					$tableName,
//					substr($sqlInserts, 0, -1)
//				);
//
//				$this->pdo->exec($sql);
//			}
		}

		$this->pdo->commit();

		$count = $this->pdo->query(sprintf('SELECT COUNT(*) AS itemsCount FROM %s', $tableName))->fetchColumn();
		$this->assertEquals($this->countTable($file), (int) $count);
	}

	/**
	 * Count records in CSV (with headers)
	 *
	 * @param CsvFile $file
	 * @return int
	 */
	protected function countTable(CsvFile $file)
	{
		$linesCount = 0;
		foreach ($file AS $i => $line)
		{
			// skip header
			if (!$i) {
				continue;
			}

			$linesCount++;
		}

		return $linesCount;
	}

	public function testCredentials()
	{
		$config = $this->getConfig('mssql');
		$config['action'] = 'testConnection';
		unset($config['parameters']['tables']);

		$app = $this->createApplication($config);
		$result = $app->run();

		$this->assertArrayHasKey('status', $result);
		$this->assertEquals('success', $result['status']);
	}

	public function testRunWithoutTables()
	{
		$config = $this->getConfig('mssql');

		unset($config['parameters']['tables']);

		$app = $this->createApplication($config);
		$result = $app->run();

		$this->assertArrayHasKey('status', $result);
		$this->assertEquals('success', $result['status']);
	}

	public function testRun()
	{
		$config = $this->getConfig('mssql');

		$app = $this->createApplication($config);

		$csv1 = new CsvFile($this->dataDir . '/mssql/sales.csv');

		// set createdat as PK
		$this->createTextTable($csv1, ['createdat']);

		$result = $app->run();

		$outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv';

		$this->assertEquals('success', $result['status']);
		$this->assertFileExists($outputCsvFile);
		$this->assertFileExists($this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest');
		$this->assertFileEquals((string) $csv1, $outputCsvFile);
	}

	public function testCredentialsWithSSH()
	{
		$config = $this->getConfig('mssql');
		$config['action'] = 'testConnection';

		$config['parameters']['db']['ssh'] = [
			'enabled' => true,
			'keys' => [
				'#private' => $this->getEnv('mssql', 'DB_SSH_KEY_PRIVATE'),
				'public' => $this->getEnv('mssql', 'DB_SSH_KEY_PUBLIC')
			],
			'user' => 'root',
			'sshHost' => 'sshproxy',
			'remoteHost' => 'mssql',
			'remotePort' => '1433',
			'localPort' => '1235',
		];

		unset($config['parameters']['tables']);

		$app = $this->createApplication($config);
		$result = $app->run();

		$this->assertArrayHasKey('status', $result);
		$this->assertEquals('success', $result['status']);
	}

	public function testRunWithSSH()
	{
		$config = $this->getConfig('mssql');
		$config['parameters']['db']['ssh'] = [
			'enabled' => true,
			'keys' => [
				'#private' => $this->getEnv('mssql', 'DB_SSH_KEY_PRIVATE'),
				'public' => $this->getEnv('mssql', 'DB_SSH_KEY_PUBLIC')
			],
			'user' => 'root',
			'sshHost' => 'sshproxy',
			'remoteHost' => 'mssql',
			'remotePort' => '1433',
			'localPort' => '1234',
		];

		$app = $this->createApplication($config);


		$csv1 = new CsvFile($this->dataDir . '/mssql/sales.csv');
		$this->createTextTable($csv1, ['createdat']);


		$result = $app->run();


		$outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv';

		$this->assertEquals('success', $result['status']);
		$this->assertFileExists($outputCsvFile);
		$this->assertFileExists($this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest');
		$this->assertFileEquals((string) $csv1, $outputCsvFile);
	}

    public function testGetTables()
    {
        $config = $this->getConfig();
        $config['action'] = 'getTables';

        $csv1 = new CsvFile($this->dataDir . '/mssql/sales.csv');
        $this->createTextTable($csv1, ['createdat']);

        $app = new Application($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertArrayHasKey('tables', $result);
        $this->assertEquals('success', $result['status']);
        $this->assertCount(1, $result['tables']);
        $this->assertArrayHasKey('name', $result['tables'][0]);
        $this->assertEquals("sales", $result['tables'][0]['name']);
        $this->assertArrayHasKey('columns', $result['tables'][0]);
        $this->assertCount(12, $result['tables'][0]['columns']);
        $this->assertArrayHasKey('name', $result['tables'][0]['columns'][0]);
        $this->assertEquals("usergender", $result['tables'][0]['columns'][0]['name']);
        $this->assertArrayHasKey('type', $result['tables'][0]['columns'][0]);
        $this->assertEquals("varchar", $result['tables'][0]['columns'][0]['type']);
        $this->assertArrayHasKey('length', $result['tables'][0]['columns'][0]);
        $this->assertEquals(255, $result['tables'][0]['columns'][0]['length']);
        $this->assertArrayHasKey('nullable', $result['tables'][0]['columns'][0]);
        $this->assertTrue($result['tables'][0]['columns'][0]['nullable']);
        $this->assertArrayHasKey('default', $result['tables'][0]['columns'][0]);
        $this->assertNull($result['tables'][0]['columns'][0]['default']);
        $this->assertArrayHasKey('primaryKey', $result['tables'][0]['columns'][0]);
        $this->assertFalse($result['tables'][0]['columns'][0]['primaryKey']);

        // note the column fetch is ordered by ordinal_position so the assertion of column index must hold.
        // also, mssql ordinal_position is 1 based
        $this->assertArrayHasKey('ordinalPosition', $result['tables'][0]['columns'][6]);
        $this->assertEquals(7, $result['tables'][0]['columns'][6]['ordinalPosition']);

        // check that the primary key is set
        $this->assertEquals('createdat', $result['tables'][0]['columns'][5]['name']);
        $this->assertArrayHasKey('primaryKey', $result['tables'][0]['columns'][5]);
        // PK cannot be nullable
        $this->assertEquals(64, $result['tables'][0]['columns'][5]['length']);
        $this->assertFalse($result['tables'][0]['columns'][5]['nullable']);
        $this->assertTrue($result['tables'][0]['columns'][5]['primaryKey']);
    }

    /**
	 * @param array $config
	 * @return MSSQLApplication
	 */
	public function createApplication(array $config)
	{
		$app = new MSSQLApplication($config, $this->dataDir);

		return $app;
	}
}
