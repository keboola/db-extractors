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
            $createUserSql = sprintf("CREATE USER %s IDENTIFIED BY %s DEFAULT TABLESPACE users", $dbConfig['user'], $dbConfig['#password']);
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
            'CREATE TABLE %s (%s) tablespace users',
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

        $csv1 = new CsvFile($this->dataDir . '/oracle/sales.csv');
        $this->createTextTable($csv1);

        $csv2 = new CsvFile($this->dataDir . '/oracle/escaping.csv');
        $this->createTextTable($csv2);

        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertArrayHasKey('tables', $result);
        $this->assertEquals('success', $result['status']);
        $this->assertCount(2, $result['tables']);

        $expectedTables = array (
            0 =>
                array (
                    'name' => 'ESCAPING',
                    'tablespaceName' => 'USERS',
                    'schema' => 'TESTER',
                    'owner' => 'TESTER',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'COL1',
                                    'type' => 'NVARCHAR2',
                                    'nullable' => true,
                                    'default' => NULL,
                                    'length' => '800',
                                    'ordinalPosition' => '1',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            1 =>
                                array (
                                    'name' => 'COL2',
                                    'type' => 'NVARCHAR2',
                                    'nullable' => true,
                                    'default' => NULL,
                                    'length' => '800',
                                    'ordinalPosition' => '2',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                        ),
                ),
            1 =>
                array (
                    'name' => 'SALES',
                    'tablespaceName' => 'USERS',
                    'schema' => 'TESTER',
                    'owner' => 'TESTER',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'USERGENDER',
                                    'type' => 'NVARCHAR2',
                                    'nullable' => true,
                                    'default' => NULL,
                                    'length' => '800',
                                    'ordinalPosition' => '1',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            1 =>
                                array (
                                    'name' => 'USERCITY',
                                    'type' => 'NVARCHAR2',
                                    'nullable' => true,
                                    'default' => NULL,
                                    'length' => '800',
                                    'ordinalPosition' => '2',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            2 =>
                                array (
                                    'name' => 'USERSENTIMENT',
                                    'type' => 'NVARCHAR2',
                                    'nullable' => true,
                                    'default' => NULL,
                                    'length' => '800',
                                    'ordinalPosition' => '3',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            3 =>
                                array (
                                    'name' => 'ZIPCODE',
                                    'type' => 'NVARCHAR2',
                                    'nullable' => true,
                                    'default' => NULL,
                                    'length' => '800',
                                    'ordinalPosition' => '4',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            4 =>
                                array (
                                    'name' => 'SKU',
                                    'type' => 'NVARCHAR2',
                                    'nullable' => true,
                                    'default' => NULL,
                                    'length' => '800',
                                    'ordinalPosition' => '5',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            5 =>
                                array (
                                    'name' => 'CREATEDAT',
                                    'type' => 'NVARCHAR2',
                                    'nullable' => true,
                                    'default' => NULL,
                                    'length' => '800',
                                    'ordinalPosition' => '6',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            6 =>
                                array (
                                    'name' => 'CATEGORY',
                                    'type' => 'NVARCHAR2',
                                    'nullable' => true,
                                    'default' => NULL,
                                    'length' => '800',
                                    'ordinalPosition' => '7',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            7 =>
                                array (
                                    'name' => 'PRICE',
                                    'type' => 'NVARCHAR2',
                                    'nullable' => true,
                                    'default' => NULL,
                                    'length' => '800',
                                    'ordinalPosition' => '8',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            8 =>
                                array (
                                    'name' => 'COUNTY',
                                    'type' => 'NVARCHAR2',
                                    'nullable' => true,
                                    'default' => NULL,
                                    'length' => '800',
                                    'ordinalPosition' => '9',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            9 =>
                                array (
                                    'name' => 'COUNTYCODE',
                                    'type' => 'NVARCHAR2',
                                    'nullable' => true,
                                    'default' => NULL,
                                    'length' => '800',
                                    'ordinalPosition' => '10',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            10 =>
                                array (
                                    'name' => 'USERSTATE',
                                    'type' => 'NVARCHAR2',
                                    'nullable' => true,
                                    'default' => NULL,
                                    'length' => '800',
                                    'ordinalPosition' => '11',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            11 =>
                                array (
                                    'name' => 'CATEGORYGROUP',
                                    'type' => 'NVARCHAR2',
                                    'nullable' => true,
                                    'default' => NULL,
                                    'length' => '800',
                                    'ordinalPosition' => '12',
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

        // use just 1 table
        unset($config['parameters']['tables'][0]);
        unset($config['parameters']['tables'][1]);

        $app = $this->createApplication($config);

        $csv1 = new CsvFile($this->dataDir . '/oracle/sales.csv');
        $this->createTextTable($csv1);

        $result = $app->run();

        $outputManifest = Yaml::parse(
            file_get_contents($this->dataDir . '/out/tables/in.c-main.tableColumns.csv.manifest')
        );

        $this->assertArrayHasKey('destination', $outputManifest);
        $this->assertArrayHasKey('incremental', $outputManifest);
        $this->assertArrayHasKey('metadata', $outputManifest);

        $expectedMetadata = array (
            0 =>
                array (
                    'key' => 'KBC.name',
                    'value' => 'SALES',
                ),
            1 =>
                array (
                    'key' => 'KBC.tablespaceName',
                    'value' => 'USERS',
                ),
            2 =>
                array (
                    'key' => 'KBC.schema',
                    'value' => 'TESTER',
                ),
            3 =>
                array (
                    'key' => 'KBC.owner',
                    'value' => 'TESTER',
                ),
        );

        $this->assertEquals($expectedMetadata, $outputManifest['metadata']);
        $this->assertArrayHasKey('column_metadata', $outputManifest);
        $this->assertCount(4, $outputManifest['column_metadata']);

        $expectedColumnMetadata = array (
            'USERGENDER' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'NVARCHAR2',
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
                            'value' => '800',
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => '1',
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
            'USERCITY' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'NVARCHAR2',
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
                            'value' => '800',
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => '2',
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
            'USERSENTIMENT' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'NVARCHAR2',
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
                            'value' => '800',
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
            'ZIPCODE' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'NVARCHAR2',
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
                            'value' => '800',
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
