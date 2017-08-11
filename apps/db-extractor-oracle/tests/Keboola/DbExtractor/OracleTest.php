<?php
/**
 * @package ex-db-oracle
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\DbExtractor;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Configuration\OracleConfigDefinition;
use Keboola\DbExtractor\Test\ExtractorTest;
use Symfony\Component\Yaml\Yaml;

class OracleTest extends ExtractorTest
{
	protected $connection;

	public function setUp()
	{
        if (!defined('APP_NAME')) {
            define('APP_NAME', 'ex-db-oracle');
        }

        $config = $this->getConfig('oracle');
        $dbConfig = $config['parameters']['db'];
        $dbString = '//' . $dbConfig['host'] . ':' . $dbConfig['port'] . '/' . $dbConfig['database'];

        $this->connection = @oci_connect($dbConfig['user'], $dbConfig['#password'], $dbString, 'AL32UTF8');
        if (!$this->connection) {
            $error = oci_error();
            echo $error['message'];
        }
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

		return $tableName;
	}

	/**
	 * Create table from csv file with text columns
	 *
	 * @param CsvFile $file
	 */
	private function createTextTable(CsvFile $file)
	{
		$tableName = $this->generateTableName($file);

		try {
			oci_execute(oci_parse($this->connection, sprintf("DROP TABLE %s", $tableName)));
		} catch (\Exception $e) {
			// table dont exists
		}

		$header = $file->getHeader();

		oci_execute(oci_parse($this->connection, sprintf(
			'CREATE TABLE %s (%s)',
			$tableName,
			implode(
				', ',
				array_map(function ($column) {
					return $column . ' NVARCHAR2 (400)';
				}, $header)
			),
			$tableName
		)));

		$file->next();

		$columnsCount = count($file->current());
		$rowsPerInsert = intval((1000 / $columnsCount) - 1);

		while ($file->current() != false) {
			for ($i=0; $i<$rowsPerInsert && $file->current() !== false; $i++) {
				$cols = [];
				foreach ($file->current() as $col) {
					$cols[] = "'" . $col . "'";
				}
				$sql = sprintf(
					"INSERT INTO {$tableName} (%s) VALUES (%s)",
					implode(',', $header),
					implode(',', $cols));

				oci_execute(oci_parse($this->connection, $sql));

				$file->next();
			}
		}

		$stmt = oci_parse($this->connection, sprintf('SELECT COUNT(*) AS ITEMSCOUNT FROM %s', $tableName));
		oci_execute($stmt);
		$row = oci_fetch_assoc($stmt);

		$this->assertEquals($this->countTable($file), (int) $row['ITEMSCOUNT']);
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
		$config = $this->getConfig('oracle');
		$config['action'] = 'testConnection';
		unset($config['parameters']['tables']);

		$app = $this->createApplication($config);
		$result = $app->run();

		$this->assertArrayHasKey('status', $result);
		$this->assertEquals('success', $result['status']);
	}

	public function testRunWithoutTables()
	{
		$config = $this->getConfig('oracle');

		unset($config['parameters']['tables']);

		$app = $this->createApplication($config);
		$result = $app->run();

		$this->assertArrayHasKey('status', $result);
		$this->assertEquals('success', $result['status']);
	}

	public function testRun()
	{
		$config = $this->getConfig('oracle');
		$app = $this->createApplication($config);

        $csv1 = new CsvFile($this->dataDir . '/oracle/sales.csv');
        $this->createTextTable($csv1);

        $csv2 = new CsvFile($this->dataDir . '/oracle/escaping.csv');
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
		$config = $this->getConfig('oracle');

		$config['parameters']['db']['ssh'] = [
			'enabled' => true,
			'keys' => [
				'#private' => $this->getEnv('mysql', 'DB_SSH_KEY_PRIVATE'),
				'public' => $this->getEnv('mysql', 'DB_SSH_KEY_PUBLIC')
			],
			'user' => 'root',
			'sshHost' => 'sshproxy',
			'remoteHost' => 'oracle',
			'remotePort' => $config['parameters']['db']['port'],
			'localPort' => '15212',
		];

		$config['action'] = 'testConnection';
		unset($config['parameters']['tables']);

		$app = $this->createApplication($config);
		$result = $app->run();

		$this->assertArrayHasKey('status', $result);
		$this->assertEquals('success', $result['status']);
	}

	public function testRunWithSSH()
	{
		$config = $this->getConfig('oracle');
		$config['parameters']['db']['ssh'] = [
			'enabled' => true,
			'keys' => [
				'#private' => $this->getEnv('mysql', 'DB_SSH_KEY_PRIVATE'),
				'public' => $this->getEnv('mysql', 'DB_SSH_KEY_PUBLIC')
			],
			'user' => 'root',
			'sshHost' => 'sshproxy',
			'remoteHost' => 'oracle',
			'remotePort' => $config['parameters']['db']['port'],
			'localPort' => '15211',
		];

		$app = $this->createApplication($config);

        $csv1 = new CsvFile($this->dataDir . '/oracle/sales.csv');
        $this->createTextTable($csv1);

        $csv2 = new CsvFile($this->dataDir . '/oracle/escaping.csv');
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

    public function testGetTables()
    {
        $config = $this->getConfig('oracle');
        $config['action'] = 'getTables';

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertArrayHasKey('tables', $result);
        $this->assertEquals('success', $result['status']);
        $this->assertCount(95, $result['tables']);

        $expectedTable0 = array (
            0 =>
                array (
                    'name' => 'AGENT_NAME',
                    'type' => 'VARCHAR2',
                    'nullable' => false,
                    'default' => NULL,
                    'length' => '30',
                    'ordinalPosition' => '1',
                    'indexed' => true,
                    'constraintName' => 'SYS_C001692',
                    'primaryKey' => false,
                    'uniqueKey' => false,
                ),
            1 =>
                array (
                    'name' => 'PROTOCOL',
                    'type' => 'NUMBER',
                    'nullable' => false,
                    'default' => NULL,
                    'length' => '22',
                    'ordinalPosition' => '2',
                    'indexed' => true,
                    'constraintName' => 'SYS_C001693',
                    'primaryKey' => false,
                    'uniqueKey' => false,
                ),
            2 =>
                array (
                    'name' => 'AGENT_NAME',
                    'type' => 'VARCHAR2',
                    'nullable' => false,
                    'default' => NULL,
                    'length' => '30',
                    'ordinalPosition' => '1',
                    'indexed' => true,
                    'constraintName' => 'SYS_C001694',
                    'primaryKey' => true,
                    'uniqueKey' => false,
                ),
            3 =>
                array (
                    'name' => 'SPARE1',
                    'type' => 'VARCHAR2',
                    'nullable' => true,
                    'default' => NULL,
                    'length' => '128',
                    'ordinalPosition' => '3',
                ),
        );
        $this->assertEquals($expectedTable0, $result['tables'][0]);

        $expectedTable1 = array (
            0 =>
                array (
                    'name' => 'AGENT_NAME',
                    'type' => 'VARCHAR2',
                    'nullable' => false,
                    'default' => NULL,
                    'length' => '30',
                    'ordinalPosition' => '1',
                    'indexed' => true,
                    'constraintName' => 'SYS_C001695',
                    'primaryKey' => false,
                    'uniqueKey' => false,
                ),
            1 =>
                array (
                    'name' => 'DB_USERNAME',
                    'type' => 'VARCHAR2',
                    'nullable' => false,
                    'default' => NULL,
                    'length' => '30',
                    'ordinalPosition' => '2',
                    'indexed' => true,
                    'constraintName' => 'SYS_C001696',
                    'primaryKey' => false,
                    'uniqueKey' => false,
                ),
            2 =>
                array (
                    'name' => 'AGENT_NAME',
                    'type' => 'VARCHAR2',
                    'nullable' => false,
                    'default' => NULL,
                    'length' => '30',
                    'ordinalPosition' => '1',
                    'indexed' => true,
                    'constraintName' => 'UNQ_PAIRS',
                    'primaryKey' => false,
                    'uniqueKey' => true,
                ),
            3 =>
                array (
                    'name' => 'DB_USERNAME',
                    'type' => 'VARCHAR2',
                    'nullable' => false,
                    'default' => NULL,
                    'length' => '30',
                    'ordinalPosition' => '2',
                    'indexed' => true,
                    'constraintName' => 'UNQ_PAIRS',
                    'primaryKey' => false,
                    'uniqueKey' => true,
                ),
            4 =>
                array (
                    'name' => 'AGENT_NAME',
                    'type' => 'VARCHAR2',
                    'nullable' => false,
                    'default' => NULL,
                    'length' => '30',
                    'ordinalPosition' => '1',
                    'indexed' => true,
                    'constraintName' => 'AGENT_MUST_BE_CREATED',
                    'primaryKey' => false,
                    'uniqueKey' => false,
                    'foreignKeyRefTable' => 'SYSTEM',
                    'foreignKeyRef' => 'SYS_C001694',
                ),
        );
        $this->assertEquals($expectedTable1, $result['tables'][1]);
    }

    public function testMetadataManifest()
    {
        $config = $this->getConfig('oracle');
        $config['parameters']['tables'][0]['columns'] = ['col1', 'col2'];
        $config['parameters']['tables'][0]['table'] = 'escaping';

        $manifestFile = $this->dataDir . '/out/tables/in.c-main.escaping.csv.manifest';
        @unlink($manifestFile);

        $app = new Application($config);

        $result = $app->run();

        $outputManifest = Yaml::parse(
            file_get_contents($manifestFile)
        );

        $this->assertArrayHasKey('destination', $outputManifest);
        $this->assertArrayHasKey('incremental', $outputManifest);
        $this->assertArrayHasKey('metadata', $outputManifest);

        $expectedMetadata = [];

        $this->assertEquals($expectedMetadata, $outputManifest['metadata']);
        $this->assertArrayHasKey('column_metadata', $outputManifest);
        $this->assertCount(2, $outputManifest['column_metadata']);

        $expectedColumnMetadata = [];

        $this->assertEquals($expectedColumnMetadata, $outputManifest['colMetadata']);
    }

    /**
	 * @param array $config
	 * @return OracleApplication
	 */
	public function createApplication(array $config)
	{
		$app = new OracleApplication($config, $this->dataDir);

		return $app;
	}
}
