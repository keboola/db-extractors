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

        $adminConnection = @oci_connect('system', 'oracle', $dbString, 'AL32UTF8');
        if (!$adminConnection) {
            $error = oci_error();
            echo $error['message'];
        }
        try {
            $createUserSql = sprintf("CREATE USER %s IDENTIFIED BY %s", $dbConfig['user'], $dbConfig['#password']);
            // create test user
            oci_execute(oci_parse(
                $adminConnection,
                $createUserSql
            ));
            // provide roles
            oci_execute(oci_parse(
                $adminConnection,
                sprintf("GRANT CONNECT,RESOURCE,DBA TO %s", $dbConfig['user'])
            ));
            // grant privileges
            oci_execute(oci_parse(
                $adminConnection,
                sprintf("GRANT CREATE SESSION GRANT ANY PRIVILEGE TO %s", $dbConfig['user'])
            ));
        } catch (\Exception $e) {
            // make sure this is the case that TESTER already exists
            if (!strstr($e->getMessage(), "ORA-01920")) {
                echo "Error creating user: " . $e->getMessage();
            }
        }
        @oci_close($adminConnection);
        $this->connection = @oci_connect($dbConfig['user'], $dbConfig['#password'], $dbString, 'AL32UTF8');
	}

	public function tearDown()
    {
        @oci_close($this->connection);
        parent::tearDown();
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
        $this->assertCount(6, $result['tables']);

        $expectedTables = array (
            0 =>
                array (
                    'name' => 'DEPARTMENTS',
                    'schema' => 'USERS',
                    'owner' => 'HR',
                    'rowCount' => '27',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'DEPARTMENT_ID',
                                    'type' => 'NUMBER',
                                    'nullable' => false,
                                    'default' => NULL,
                                    'length' => '4,0',
                                    'ordinalPosition' => '1',
                                    'primaryKey' => true,
                                    'uniqueKey' => false,
                                    'primaryKeyName' => 'DEPT_ID_PK',
                                ),
                            1 =>
                                array (
                                    'name' => 'DEPARTMENT_NAME',
                                    'type' => 'VARCHAR2',
                                    'nullable' => false,
                                    'default' => NULL,
                                    'length' => '30',
                                    'ordinalPosition' => '2',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            2 =>
                                array (
                                    'name' => 'MANAGER_ID',
                                    'type' => 'NUMBER',
                                    'nullable' => true,
                                    'default' => NULL,
                                    'length' => '6,0',
                                    'ordinalPosition' => '3',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                    'foreignKeyName' => 'DEPT_MGR_FK',
                                    'foreignKeyRefTable' => 'HR',
                                    'foreignKeyRef' => 'EMP_EMP_ID_PK',
                                ),
                            3 =>
                                array (
                                    'name' => 'LOCATION_ID',
                                    'type' => 'NUMBER',
                                    'nullable' => true,
                                    'default' => NULL,
                                    'length' => '4,0',
                                    'ordinalPosition' => '4',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                    'foreignKeyName' => 'DEPT_LOC_FK',
                                    'foreignKeyRefTable' => 'HR',
                                    'foreignKeyRef' => 'LOC_ID_PK',
                                ),
                        ),
                ),
            1 =>
                array (
                    'name' => 'EMPLOYEES',
                    'schema' => 'USERS',
                    'owner' => 'HR',
                    'rowCount' => '107',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'EMPLOYEE_ID',
                                    'type' => 'NUMBER',
                                    'nullable' => false,
                                    'default' => NULL,
                                    'length' => '6,0',
                                    'ordinalPosition' => '1',
                                    'primaryKey' => true,
                                    'uniqueKey' => false,
                                    'primaryKeyName' => 'EMP_EMP_ID_PK',
                                ),
                            1 =>
                                array (
                                    'name' => 'FIRST_NAME',
                                    'type' => 'VARCHAR2',
                                    'nullable' => true,
                                    'default' => NULL,
                                    'length' => '20',
                                    'ordinalPosition' => '2',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            2 =>
                                array (
                                    'name' => 'LAST_NAME',
                                    'type' => 'VARCHAR2',
                                    'nullable' => false,
                                    'default' => NULL,
                                    'length' => '25',
                                    'ordinalPosition' => '3',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            3 =>
                                array (
                                    'name' => 'EMAIL',
                                    'type' => 'VARCHAR2',
                                    'nullable' => false,
                                    'default' => NULL,
                                    'length' => '25',
                                    'ordinalPosition' => '4',
                                    'primaryKey' => false,
                                    'uniqueKey' => true,
                                    'uniqueKeyName' => 'EMP_EMAIL_UK',
                                ),
                            4 =>
                                array (
                                    'name' => 'PHONE_NUMBER',
                                    'type' => 'VARCHAR2',
                                    'nullable' => true,
                                    'default' => NULL,
                                    'length' => '20',
                                    'ordinalPosition' => '5',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            5 =>
                                array (
                                    'name' => 'HIRE_DATE',
                                    'type' => 'DATE',
                                    'nullable' => false,
                                    'default' => NULL,
                                    'length' => '7',
                                    'ordinalPosition' => '6',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            6 =>
                                array (
                                    'name' => 'JOB_ID',
                                    'type' => 'VARCHAR2',
                                    'nullable' => false,
                                    'default' => NULL,
                                    'length' => '10',
                                    'ordinalPosition' => '7',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                    'foreignKeyName' => 'EMP_JOB_FK',
                                    'foreignKeyRefTable' => 'HR',
                                    'foreignKeyRef' => 'JOB_ID_PK',
                                ),
                            7 =>
                                array (
                                    'name' => 'SALARY',
                                    'type' => 'NUMBER',
                                    'nullable' => true,
                                    'default' => NULL,
                                    'length' => '8,2',
                                    'ordinalPosition' => '8',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            8 =>
                                array (
                                    'name' => 'COMMISSION_PCT',
                                    'type' => 'NUMBER',
                                    'nullable' => true,
                                    'default' => NULL,
                                    'length' => '2,2',
                                    'ordinalPosition' => '9',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            9 =>
                                array (
                                    'name' => 'MANAGER_ID',
                                    'type' => 'NUMBER',
                                    'nullable' => true,
                                    'default' => NULL,
                                    'length' => '6,0',
                                    'ordinalPosition' => '10',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                    'foreignKeyName' => 'EMP_MANAGER_FK',
                                    'foreignKeyRefTable' => 'HR',
                                    'foreignKeyRef' => 'EMP_EMP_ID_PK',
                                ),
                            10 =>
                                array (
                                    'name' => 'DEPARTMENT_ID',
                                    'type' => 'NUMBER',
                                    'nullable' => true,
                                    'default' => NULL,
                                    'length' => '4,0',
                                    'ordinalPosition' => '11',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                    'foreignKeyName' => 'EMP_DEPT_FK',
                                    'foreignKeyRefTable' => 'HR',
                                    'foreignKeyRef' => 'DEPT_ID_PK',
                                ),
                        ),
                ),
            2 =>
                array (
                    'name' => 'JOBS',
                    'schema' => 'USERS',
                    'owner' => 'HR',
                    'rowCount' => '19',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'JOB_ID',
                                    'type' => 'VARCHAR2',
                                    'nullable' => false,
                                    'default' => NULL,
                                    'length' => '10',
                                    'ordinalPosition' => '1',
                                    'primaryKey' => true,
                                    'uniqueKey' => false,
                                    'primaryKeyName' => 'JOB_ID_PK',
                                ),
                            1 =>
                                array (
                                    'name' => 'JOB_TITLE',
                                    'type' => 'VARCHAR2',
                                    'nullable' => false,
                                    'default' => NULL,
                                    'length' => '35',
                                    'ordinalPosition' => '2',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            2 =>
                                array (
                                    'name' => 'MIN_SALARY',
                                    'type' => 'NUMBER',
                                    'nullable' => true,
                                    'default' => NULL,
                                    'length' => '6,0',
                                    'ordinalPosition' => '3',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            3 =>
                                array (
                                    'name' => 'MAX_SALARY',
                                    'type' => 'NUMBER',
                                    'nullable' => true,
                                    'default' => NULL,
                                    'length' => '6,0',
                                    'ordinalPosition' => '4',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                        ),
                ),
            3 =>
                array (
                    'name' => 'JOB_HISTORY',
                    'schema' => 'USERS',
                    'owner' => 'HR',
                    'rowCount' => '10',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'EMPLOYEE_ID',
                                    'type' => 'NUMBER',
                                    'nullable' => false,
                                    'default' => NULL,
                                    'length' => '6,0',
                                    'ordinalPosition' => '1',
                                    'primaryKey' => true,
                                    'uniqueKey' => false,
                                    'primaryKeyName' => 'JHIST_EMP_ID_ST_DATE_PK',
                                    'foreignKeyName' => 'JHIST_EMP_FK',
                                    'foreignKeyRefTable' => 'HR',
                                    'foreignKeyRef' => 'EMP_EMP_ID_PK',
                                ),
                            1 =>
                                array (
                                    'name' => 'START_DATE',
                                    'type' => 'DATE',
                                    'nullable' => false,
                                    'default' => NULL,
                                    'length' => '7',
                                    'ordinalPosition' => '2',
                                    'primaryKey' => true,
                                    'uniqueKey' => false,
                                    'primaryKeyName' => 'JHIST_EMP_ID_ST_DATE_PK',
                                ),
                            2 =>
                                array (
                                    'name' => 'END_DATE',
                                    'type' => 'DATE',
                                    'nullable' => false,
                                    'default' => NULL,
                                    'length' => '7',
                                    'ordinalPosition' => '3',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            3 =>
                                array (
                                    'name' => 'JOB_ID',
                                    'type' => 'VARCHAR2',
                                    'nullable' => false,
                                    'default' => NULL,
                                    'length' => '10',
                                    'ordinalPosition' => '4',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                    'foreignKeyName' => 'JHIST_JOB_FK',
                                    'foreignKeyRefTable' => 'HR',
                                    'foreignKeyRef' => 'JOB_ID_PK',
                                ),
                            4 =>
                                array (
                                    'name' => 'DEPARTMENT_ID',
                                    'type' => 'NUMBER',
                                    'nullable' => true,
                                    'default' => NULL,
                                    'length' => '4,0',
                                    'ordinalPosition' => '5',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                    'foreignKeyName' => 'JHIST_DEPT_FK',
                                    'foreignKeyRefTable' => 'HR',
                                    'foreignKeyRef' => 'DEPT_ID_PK',
                                ),
                        ),
                ),
            4 =>
                array (
                    'name' => 'LOCATIONS',
                    'schema' => 'USERS',
                    'owner' => 'HR',
                    'rowCount' => '23',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'LOCATION_ID',
                                    'type' => 'NUMBER',
                                    'nullable' => false,
                                    'default' => NULL,
                                    'length' => '4,0',
                                    'ordinalPosition' => '1',
                                    'primaryKey' => true,
                                    'uniqueKey' => false,
                                    'primaryKeyName' => 'LOC_ID_PK',
                                ),
                            1 =>
                                array (
                                    'name' => 'STREET_ADDRESS',
                                    'type' => 'VARCHAR2',
                                    'nullable' => true,
                                    'default' => NULL,
                                    'length' => '40',
                                    'ordinalPosition' => '2',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            2 =>
                                array (
                                    'name' => 'POSTAL_CODE',
                                    'type' => 'VARCHAR2',
                                    'nullable' => true,
                                    'default' => NULL,
                                    'length' => '12',
                                    'ordinalPosition' => '3',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            3 =>
                                array (
                                    'name' => 'CITY',
                                    'type' => 'VARCHAR2',
                                    'nullable' => false,
                                    'default' => NULL,
                                    'length' => '30',
                                    'ordinalPosition' => '4',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            4 =>
                                array (
                                    'name' => 'STATE_PROVINCE',
                                    'type' => 'VARCHAR2',
                                    'nullable' => true,
                                    'default' => NULL,
                                    'length' => '25',
                                    'ordinalPosition' => '5',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            5 =>
                                array (
                                    'name' => 'COUNTRY_ID',
                                    'type' => 'CHAR',
                                    'nullable' => true,
                                    'default' => NULL,
                                    'length' => '2',
                                    'ordinalPosition' => '6',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                    'foreignKeyName' => 'LOC_C_ID_FK',
                                    'foreignKeyRefTable' => 'HR',
                                    'foreignKeyRef' => 'COUNTRY_C_ID_PK',
                                ),
                        ),
                ),
            5 =>
                array (
                    'name' => 'REGIONS',
                    'schema' => 'USERS',
                    'owner' => 'HR',
                    'rowCount' => '4',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'REGION_ID',
                                    'type' => 'NUMBER',
                                    'nullable' => false,
                                    'default' => NULL,
                                    'length' => '22',
                                    'ordinalPosition' => '1',
                                    'primaryKey' => true,
                                    'uniqueKey' => false,
                                    'primaryKeyName' => 'REG_ID_PK',
                                ),
                            1 =>
                                array (
                                    'name' => 'REGION_NAME',
                                    'type' => 'VARCHAR2',
                                    'nullable' => true,
                                    'default' => NULL,
                                    'length' => '25',
                                    'ordinalPosition' => '2',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                        ),
                ),
        );
        $this->assertEquals($expectedTables, $result['tables']);
    }

    public function testMetadataManifest()
    {
        $config = $this->getConfig('oracle');

        $config['parameters']['tables'][0]['columns'] = ['EMPLOYEE_ID', 'START_DATE', 'END_DATE', 'JOB_ID', 'DEPARTMENT_ID'];
        $config['parameters']['tables'][0]['table'] = 'JOB_HISTORY';
        $config['parameters']['tables'][0]['outputTable'] = "in.c-main.JOB_HISTORY";
        // use just 1 table
        unset($config['parameters']['tables'][1]);
        unset($config['parameters']['tables'][2]);

        $app = $this->createApplication($config);

        $result = $app->run();

        $outputManifest = Yaml::parse(
            file_get_contents($this->dataDir . '/out/tables/in.c-main.JOB_HISTORY.csv.manifest')
        );

        $this->assertArrayHasKey('destination', $outputManifest);
        $this->assertArrayHasKey('incremental', $outputManifest);
        $this->assertArrayHasKey('metadata', $outputManifest);

        $expectedMetadata = array (
            0 =>
                array (
                    'key' => 'KBC.name',
                    'value' => 'JOB_HISTORY',
                ),
            1 =>
                array (
                    'key' => 'KBC.schema',
                    'value' => 'USERS',
                ),
            2 =>
                array (
                    'key' => 'KBC.owner',
                    'value' => 'HR',
                ),
            3 =>
                array (
                    'key' => 'KBC.rowCount',
                    'value' => '10',
                ),
        );

        $this->assertEquals($expectedMetadata, $outputManifest['metadata']);
        $this->assertArrayHasKey('column_metadata', $outputManifest);
        $this->assertCount(5, $outputManifest['column_metadata']);

        $expectedColumnMetadata = array (
            'EMPLOYEE_ID' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'NUMBER',
                        ),
                    1 =>
                        array (
                            'key' => 'KBC.datatype.nullable',
                            'value' => false,
                        ),
                    2 =>
                        array (
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'NUMERIC',
                        ),
                    3 =>
                        array (
                            'key' => 'KBC.datatype.length',
                            'value' => '6,0',
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => '1',
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
                            'key' => 'KBC.primaryKeyName',
                            'value' => 'JHIST_EMP_ID_ST_DATE_PK',
                        ),
                    8 =>
                        array (
                            'key' => 'KBC.foreignKeyName',
                            'value' => 'JHIST_EMP_FK',
                        ),
                    9 =>
                        array (
                            'key' => 'KBC.foreignKeyRefTable',
                            'value' => 'HR',
                        ),
                    10 =>
                        array (
                            'key' => 'KBC.foreignKeyRef',
                            'value' => 'EMP_EMP_ID_PK',
                        ),
                ),
            'START_DATE' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'DATE',
                        ),
                    1 =>
                        array (
                            'key' => 'KBC.datatype.nullable',
                            'value' => false,
                        ),
                    2 =>
                        array (
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'DATE',
                        ),
                    3 =>
                        array (
                            'key' => 'KBC.datatype.length',
                            'value' => '7',
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => '2',
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
                            'key' => 'KBC.primaryKeyName',
                            'value' => 'JHIST_EMP_ID_ST_DATE_PK',
                        ),
                ),
            'END_DATE' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'DATE',
                        ),
                    1 =>
                        array (
                            'key' => 'KBC.datatype.nullable',
                            'value' => false,
                        ),
                    2 =>
                        array (
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'DATE',
                        ),
                    3 =>
                        array (
                            'key' => 'KBC.datatype.length',
                            'value' => '7',
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => '3',
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.primaryKey',
                            'value' => false,
                        ),
                    6 =>
                        array (
                            'key' => 'KBC.uniqueKey',
                            'value' => false,
                        ),
                ),
            'JOB_ID' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'VARCHAR2',
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
                            'value' => '10',
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => '4',
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.primaryKey',
                            'value' => false,
                        ),
                    6 =>
                        array (
                            'key' => 'KBC.uniqueKey',
                            'value' => false,
                        ),
                    7 =>
                        array (
                            'key' => 'KBC.foreignKeyName',
                            'value' => 'JHIST_JOB_FK',
                        ),
                    8 =>
                        array (
                            'key' => 'KBC.foreignKeyRefTable',
                            'value' => 'HR',
                        ),
                    9 =>
                        array (
                            'key' => 'KBC.foreignKeyRef',
                            'value' => 'JOB_ID_PK',
                        ),
                ),
            'DEPARTMENT_ID' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'NUMBER',
                        ),
                    1 =>
                        array (
                            'key' => 'KBC.datatype.nullable',
                            'value' => true,
                        ),
                    2 =>
                        array (
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'NUMERIC',
                        ),
                    3 =>
                        array (
                            'key' => 'KBC.datatype.length',
                            'value' => '4,0',
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => '5',
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.primaryKey',
                            'value' => false,
                        ),
                    6 =>
                        array (
                            'key' => 'KBC.uniqueKey',
                            'value' => false,
                        ),
                    7 =>
                        array (
                            'key' => 'KBC.foreignKeyName',
                            'value' => 'JHIST_DEPT_FK',
                        ),
                    8 =>
                        array (
                            'key' => 'KBC.foreignKeyRefTable',
                            'value' => 'HR',
                        ),
                    9 =>
                        array (
                            'key' => 'KBC.foreignKeyRef',
                            'value' => 'DEPT_ID_PK',
                        ),
                ),
        );
        $this->assertEquals($expectedColumnMetadata, $outputManifest['column_metadata']);
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
