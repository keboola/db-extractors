<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Extractor\Oracle;
use Keboola\DbExtractorLogger\Logger;

class OracleTest extends OracleBaseTest
{
    public function testCredentials(): void
    {
        $config = $this->getConfig('oracle');
        $config['action'] = 'testConnection';
        unset($config['parameters']['tables']);

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    public function testRunWithoutTables(): void
    {
        $config = $this->getConfig('oracle');

        unset($config['parameters']['tables']);

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    public function testRunRowConfig(): void
    {
        $config = $this->getConfigRow('oracle');
        $app = $this->createApplication($config);
        $this->setupTestRowTable();

        $result = $app->run();
        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported']['outputTable'] . '.csv';

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists(
            $this->dataDir . '/out/tables/' . $result['imported']['outputTable'] . '.csv.manifest'
        );
        $this->assertEquals(99, $result['imported']['rows']);

        // will check this one line by line because it randomly orders it sometimes
        $output = (string) file_get_contents($outputCsvFile);
        $outputLines = explode("\n", $output);
        $origContents = (string) file_get_contents($this->dataDir . '/oracle/sales.csv');
        foreach ($outputLines as $line) {
            if (trim($line) !== '') {
                $this->assertStringContainsString($line, $origContents);
            }
        }
    }

    public function testRunConfig(): void
    {
        $config = $this->getConfig('oracle');
        $app = $this->createApplication($config);
        $this->setupTestTables();

        $result = $app->run();
        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv';

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists(
            $this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv.manifest'
        );
        $this->assertEquals(99, $result['imported'][0]['rows']);

        // will check this one line by line because it randomly orders it sometimes
        $output = (string) file_get_contents($outputCsvFile);
        $outputLines = explode("\n", $output);
        $origContents = (string) file_get_contents($this->dataDir . '/oracle/sales.csv');
        foreach ($outputLines as $line) {
            if (trim($line) !== '') {
                $this->assertStringContainsString($line, $origContents);
            }
        }

        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][1]['outputTable'] . '.csv';

        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists(
            $this->dataDir . '/out/tables/' . $result['imported'][1]['outputTable'] . '.csv.manifest'
        );

        $fileNameManifest = $this->dataDir . '/out/tables/' . $result['imported'][1]['outputTable'] . '.csv.manifest';
        $manifest = json_decode(
            (string) file_get_contents($fileNameManifest),
            true
        );
        $this->assertEquals(['funnY_col', 's_d_col'], $manifest['columns']);
        $this->assertEquals(['funnY_col'], $manifest['primary_key']);
        $this->assertEquals(7, $result['imported'][1]['rows']);
        $this->assertEquals(
            file_get_contents($this->dataDir . '/oracle/headerlessEscaping.csv'),
            file_get_contents($outputCsvFile)
        );

        $outputCsvFile = $this->dataDir . '/out/tables/' . strtolower($result['imported'][2]['outputTable']) . '.csv';
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists(
            $outputCsvFile . '.manifest'
        );
        $this->assertEquals(99, $result['imported'][2]['rows']);
        $output = (string) file_get_contents($outputCsvFile);
        $outputLines = explode("\n", $output);
        $origContents = (string) file_get_contents($this->dataDir . '/oracle/tableColumns.csv');
        foreach ($outputLines as $line) {
            if (trim($line) !== '') {
                $this->assertStringContainsString($line, $origContents);
            }
        }

        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][3]['outputTable'] . '.csv';
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists(
            $this->dataDir . '/out/tables/' . $result['imported'][3]['outputTable'] . '.csv.manifest'
        );
        $this->assertEquals(4, $result['imported'][3]['rows']);
        $this->assertEquals(
            file_get_contents($this->dataDir . '/oracle/regions.csv'),
            file_get_contents($outputCsvFile)
        );
    }

    public function testCredentialsWithSSH(): void
    {
        $config = $this->getConfig('oracle');

        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getPrivateKey(),
                'public' => $this->getPublicKey(),
            ],
            'user' => 'root',
            'sshHost' => 'sshproxy',
            'remoteHost' => 'oracle',
            'remotePort' => $config['parameters']['db']['port'],
            'localPort' => '15211',
        ];

        $config['action'] = 'testConnection';
        unset($config['parameters']['tables']);

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    public function testRunWithSSH(): void
    {
        $config = $this->getConfig('oracle');
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getPrivateKey(),
                'public' => $this->getPublicKey(),
            ],
            'user' => 'root',
            'sshHost' => 'sshproxy',
            'remoteHost' => 'oracle',
            'remotePort' => $config['parameters']['db']['port'],
            'localPort' => '15212',
        ];

        $app = $this->createApplication($config);

        $this->setupTestTables();

        $salesCsv = new CsvFile($this->dataDir. '/oracle/sales.csv');
        $escapingCsv = new CsvFile($this->dataDir. '/oracle/headerlessEscaping.csv');

        $result = $app->run();

        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv';

        $this->assertEquals('success', $result['status']);

        $this->assertFileExists($outputCsvFile);
        $filenameManifest = $this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv.manifest';
        $this->assertFileExists($filenameManifest);
        // will check this one line by line because it randomly orders it sometimes
        $output = (string) file_get_contents($outputCsvFile);
        $outputLines = explode("\n", $output);
        $origContents = (string) file_get_contents($salesCsv->getPathname());
        foreach ($outputLines as $line) {
            if (trim($line) !== '') {
                $this->assertStringContainsString($line, $origContents);
            }
        }

        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][1]['outputTable'] . '.csv';
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($outputCsvFile . '.manifest');
        $this->assertEquals(file_get_contents($escapingCsv->getPathname()), file_get_contents($outputCsvFile));
    }

    public function testExtractorGetTablesWithSchema(): void
    {
        $config = $this->getConfig('oracle');
        $config['parameters']['db']['password'] = $config['parameters']['db']['#password'];
        $config['parameters']['tables'] = [];

        $extractor = new Oracle($config['parameters'], [], new Logger('ex-db-mysql-tests'));

        $this->assertTrue($extractor->testConnection());

        $this->assertGreaterThan(2, $extractor->getTables());

        $tableName = 'REGIONS';

        // get table from HR schema
        $tables = $extractor->getTables(
            [
                [
                    'tableName' => $tableName,
                    'schema' => 'HR',
                ],
            ]
        );

        $this->assertCount(1, $tables);
        $table = $tables[0];

        $this->assertArrayHasKey('name', $table);
        $this->assertSame($tableName, $table['name']);

        $this->assertArrayHasKey('schema', $table);
        $this->assertSame('HR', $table['schema']);

        $this->assertArrayHasKey('columns', $table);
        $this->assertCount(2, $table['columns']);

        // get table from user schema
        $userSchema = mb_strtoupper($config['parameters']['db']['user']);

        $tables = $extractor->getTables(
            [
                [
                    'tableName' => $tableName,
                    'schema' => $userSchema,
                ],
            ]
        );

        $this->assertCount(1, $tables);
        $table = $tables[0];

        $this->assertArrayHasKey('name', $table);
        $this->assertSame($tableName, $table['name']);

        $this->assertArrayHasKey('schema', $table);
        $this->assertSame($userSchema, $table['schema']);

        $this->assertArrayHasKey('columns', $table);
        $this->assertCount(1, $table['columns']);

        // get tables from both schemas
        $tables = $extractor->getTables(
            [
                [
                    'tableName' => $tableName,
                    'schema' => 'HR',
                ],
                [
                    'tableName' => $tableName,
                    'schema' => $userSchema,
                ],
            ]
        );

        $this->assertCount(2, $tables);
    }

    public function testGetTables(): void
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
        $this->assertCount(10, $result['tables']);

        $expectedTables = array (
            0 =>
                array (
                    'name' => 'DEPARTMENTS',
                    'tablespaceName' => 'USERS',
                    'schema' => 'HR',
                    'owner' => 'HR',
                    'rowCount' => 27,
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'DEPARTMENT_ID',
                                    'sanitizedName' => 'DEPARTMENT_ID',
                                    'type' => 'NUMBER',
                                    'nullable' => false,
                                    'length' => '4,0',
                                    'ordinalPosition' => '1',
                                    'primaryKey' => true,
                                    'uniqueKey' => false,
                                    'primaryKeyName' => 'DEPT_ID_PK',
                                ),
                            1 =>
                                array (
                                    'name' => 'DEPARTMENT_NAME',
                                    'sanitizedName' => 'DEPARTMENT_NAME',
                                    'type' => 'VARCHAR2',
                                    'nullable' => false,
                                    'length' => '30',
                                    'ordinalPosition' => '2',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            2 =>
                                array (
                                    'name' => 'MANAGER_ID',
                                    'sanitizedName' => 'MANAGER_ID',
                                    'type' => 'NUMBER',
                                    'nullable' => true,
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
                                    'sanitizedName' => 'LOCATION_ID',
                                    'type' => 'NUMBER',
                                    'nullable' => true,
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
                    'tablespaceName' => 'USERS',
                    'schema' => 'HR',
                    'owner' => 'HR',
                    'rowCount' => 107,
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'EMPLOYEE_ID',
                                    'sanitizedName' => 'EMPLOYEE_ID',
                                    'type' => 'NUMBER',
                                    'nullable' => false,
                                    'length' => '6,0',
                                    'ordinalPosition' => '1',
                                    'primaryKey' => true,
                                    'uniqueKey' => false,
                                    'primaryKeyName' => 'EMP_EMP_ID_PK',
                                ),
                            1 =>
                                array (
                                    'name' => 'FIRST_NAME',
                                    'sanitizedName' => 'FIRST_NAME',
                                    'type' => 'VARCHAR2',
                                    'nullable' => true,
                                    'length' => '20',
                                    'ordinalPosition' => '2',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            2 =>
                                array (
                                    'name' => 'LAST_NAME',
                                    'sanitizedName' => 'LAST_NAME',
                                    'type' => 'VARCHAR2',
                                    'nullable' => false,
                                    'length' => '25',
                                    'ordinalPosition' => '3',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            3 =>
                                array (
                                    'name' => 'EMAIL',
                                    'sanitizedName' => 'EMAIL',
                                    'type' => 'VARCHAR2',
                                    'nullable' => false,
                                    'length' => '25',
                                    'ordinalPosition' => '4',
                                    'primaryKey' => false,
                                    'uniqueKey' => true,
                                    'uniqueKeyName' => 'EMP_EMAIL_UK',
                                ),
                            4 =>
                                array (
                                    'name' => 'PHONE_NUMBER',
                                    'sanitizedName' => 'PHONE_NUMBER',
                                    'type' => 'VARCHAR2',
                                    'nullable' => true,
                                    'length' => '20',
                                    'ordinalPosition' => '5',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            5 =>
                                array (
                                    'name' => 'HIRE_DATE',
                                    'sanitizedName' => 'HIRE_DATE',
                                    'type' => 'DATE',
                                    'nullable' => false,
                                    'length' => null,
                                    'ordinalPosition' => '6',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            6 =>
                                array (
                                    'name' => 'JOB_ID',
                                    'sanitizedName' => 'JOB_ID',
                                    'type' => 'VARCHAR2',
                                    'nullable' => false,
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
                                    'sanitizedName' => 'SALARY',
                                    'type' => 'NUMBER',
                                    'nullable' => true,
                                    'length' => '8,2',
                                    'ordinalPosition' => '8',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            8 =>
                                array (
                                    'name' => 'COMMISSION_PCT',
                                    'sanitizedName' => 'COMMISSION_PCT',
                                    'type' => 'NUMBER',
                                    'nullable' => true,
                                    'length' => '2,2',
                                    'ordinalPosition' => '9',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            9 =>
                                array (
                                    'name' => 'MANAGER_ID',
                                    'sanitizedName' => 'MANAGER_ID',
                                    'type' => 'NUMBER',
                                    'nullable' => true,
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
                                    'sanitizedName' => 'DEPARTMENT_ID',
                                    'type' => 'NUMBER',
                                    'nullable' => true,
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
                    'tablespaceName' => 'USERS',
                    'schema' => 'HR',
                    'owner' => 'HR',
                    'rowCount' => 19,
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'JOB_ID',
                                    'sanitizedName' => 'JOB_ID',
                                    'type' => 'VARCHAR2',
                                    'nullable' => false,
                                    'length' => '10',
                                    'ordinalPosition' => '1',
                                    'primaryKey' => true,
                                    'uniqueKey' => false,
                                    'primaryKeyName' => 'JOB_ID_PK',
                                ),
                            1 =>
                                array (
                                    'name' => 'JOB_TITLE',
                                    'sanitizedName' => 'JOB_TITLE',
                                    'type' => 'VARCHAR2',
                                    'nullable' => false,
                                    'length' => '35',
                                    'ordinalPosition' => '2',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            2 =>
                                array (
                                    'name' => 'MIN_SALARY',
                                    'sanitizedName' => 'MIN_SALARY',
                                    'type' => 'NUMBER',
                                    'nullable' => true,
                                    'length' => '6,0',
                                    'ordinalPosition' => '3',
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            3 =>
                                array (
                                    'name' => 'MAX_SALARY',
                                    'sanitizedName' => 'MAX_SALARY',
                                    'type' => 'NUMBER',
                                    'nullable' => true,
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
                    'tablespaceName' => 'USERS',
                    'schema' => 'HR',
                    'owner' => 'HR',
                    'rowCount' => 10,
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'EMPLOYEE_ID',
                                    'sanitizedName' => 'EMPLOYEE_ID',
                                    'type' => 'NUMBER',
                                    'nullable' => false,
                                    'length' => '6,0',
                                    'ordinalPosition' => 1,
                                    'primaryKey' => true,
                                    'uniqueKey' => false,
                                    'foreignKeyName' => 'JHIST_EMP_FK',
                                    'foreignKeyRefTable' => 'HR',
                                    'foreignKeyRef' => 'EMP_EMP_ID_PK',
                                    'primaryKeyName' => 'JHIST_EMP_ID_ST_DATE_PK',
                                ),
                            1 =>
                                array (
                                    'name' => 'START_DATE',
                                    'sanitizedName' => 'START_DATE',
                                    'type' => 'DATE',
                                    'nullable' => false,
                                    'length' => null,
                                    'ordinalPosition' => 2,
                                    'primaryKey' => true,
                                    'uniqueKey' => false,
                                    'primaryKeyName' => 'JHIST_EMP_ID_ST_DATE_PK',
                                ),
                            2 =>
                                array (
                                    'name' => 'END_DATE',
                                    'sanitizedName' => 'END_DATE',
                                    'type' => 'DATE',
                                    'nullable' => false,
                                    'length' => null,
                                    'ordinalPosition' => 3,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            3 =>
                                array (
                                    'name' => 'JOB_ID',
                                    'sanitizedName' => 'JOB_ID',
                                    'type' => 'VARCHAR2',
                                    'nullable' => false,
                                    'length' => '10',
                                    'ordinalPosition' => 4,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                    'foreignKeyName' => 'JHIST_JOB_FK',
                                    'foreignKeyRefTable' => 'HR',
                                    'foreignKeyRef' => 'JOB_ID_PK',
                                ),
                            4 =>
                                array (
                                    'name' => 'DEPARTMENT_ID',
                                    'sanitizedName' => 'DEPARTMENT_ID',
                                    'type' => 'NUMBER',
                                    'nullable' => true,
                                    'length' => '4,0',
                                    'ordinalPosition' => 5,
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
                    'tablespaceName' => 'USERS',
                    'schema' => 'HR',
                    'owner' => 'HR',
                    'rowCount' => 23,
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'LOCATION_ID',
                                    'sanitizedName' => 'LOCATION_ID',
                                    'type' => 'NUMBER',
                                    'nullable' => false,
                                    'length' => '4,0',
                                    'ordinalPosition' => 1,
                                    'primaryKey' => true,
                                    'uniqueKey' => false,
                                    'primaryKeyName' => 'LOC_ID_PK',
                                ),
                            1 =>
                                array (
                                    'name' => 'STREET_ADDRESS',
                                    'sanitizedName' => 'STREET_ADDRESS',
                                    'type' => 'VARCHAR2',
                                    'nullable' => true,
                                    'length' => '40',
                                    'ordinalPosition' => 2,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            2 =>
                                array (
                                    'name' => 'POSTAL_CODE',
                                    'sanitizedName' => 'POSTAL_CODE',
                                    'type' => 'VARCHAR2',
                                    'nullable' => true,
                                    'length' => '12',
                                    'ordinalPosition' => 3,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            3 =>
                                array (
                                    'name' => 'CITY',
                                    'sanitizedName' => 'CITY',
                                    'type' => 'VARCHAR2',
                                    'nullable' => false,
                                    'length' => '30',
                                    'ordinalPosition' => 4,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            4 =>
                                array (
                                    'name' => 'STATE_PROVINCE',
                                    'sanitizedName' => 'STATE_PROVINCE',
                                    'type' => 'VARCHAR2',
                                    'nullable' => true,
                                    'length' => '25',
                                    'ordinalPosition' => 5,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            5 =>
                                array (
                                    'name' => 'COUNTRY_ID',
                                    'sanitizedName' => 'COUNTRY_ID',
                                    'type' => 'CHAR',
                                    'nullable' => true,
                                    'length' => '2',
                                    'ordinalPosition' => 6,
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
                    'tablespaceName' => 'USERS',
                    'schema' => 'HR',
                    'owner' => 'HR',
                    'rowCount' => 4,
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'REGION_ID',
                                    'sanitizedName' => 'REGION_ID',
                                    'type' => 'NUMBER',
                                    'nullable' => false,
                                    'length' => null,
                                    'ordinalPosition' => 1,
                                    'primaryKey' => true,
                                    'uniqueKey' => false,
                                    'primaryKeyName' => 'REG_ID_PK',
                                ),
                            1 =>
                                array (
                                    'name' => 'REGION_NAME',
                                    'sanitizedName' => 'REGION_NAME',
                                    'type' => 'VARCHAR2',
                                    'nullable' => true,
                                    'length' => '25',
                                    'ordinalPosition' => 2,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                        ),
                ),
            6 =>
                array (
                    'name' => 'CLOB_TEST',
                    'tablespaceName' => 'USERS',
                    'schema' => 'TESTER',
                    'owner' => 'TESTER',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'ID',
                                    'sanitizedName' => 'ID',
                                    'type' => 'VARCHAR2',
                                    'nullable' => true,
                                    'length' => '25',
                                    'ordinalPosition' => 1,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            1 =>
                                array (
                                    'name' => 'CLOB_COL',
                                    'sanitizedName' => 'CLOB_COL',
                                    'type' => 'CLOB',
                                    'nullable' => true,
                                    'length' => null,
                                    'ordinalPosition' => 2,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                        ),
                ),
            7 =>
                array (
                    'name' => 'ESCAPING',
                    'tablespaceName' => 'USERS',
                    'schema' => 'TESTER',
                    'owner' => 'TESTER',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => '_funnY#-col',
                                    'sanitizedName' => 'funnY_col',
                                    'type' => 'NVARCHAR2',
                                    'nullable' => true,
                                    'length' => '400',
                                    'ordinalPosition' => 1,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            1 =>
                                array (
                                    'name' => '_s%d-col',
                                    'sanitizedName' => 's_d_col',
                                    'type' => 'NVARCHAR2',
                                    'nullable' => true,
                                    'length' => '400',
                                    'ordinalPosition' => 2,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                        ),
                ),
            8 =>
                array (
                    'name' => 'REGIONS',
                    'tablespaceName' => 'USERS',
                    'schema' => 'TESTER',
                    'owner' => 'TESTER',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'REGION_ID',
                                    'sanitizedName' => 'REGION_ID',
                                    'type' => 'NUMBER',
                                    'nullable' => false,
                                    'length' => null,
                                    'ordinalPosition' => 1,
                                    'primaryKey' => true,
                                    'uniqueKey' => false,
                                    'primaryKeyName' => 'REG_ID_PK',
                                ),
                        ),
                ),
            9 =>
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
                                    'sanitizedName' => 'USERGENDER',
                                    'type' => 'NVARCHAR2',
                                    'nullable' => true,
                                    'length' => '400',
                                    'ordinalPosition' => 1,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            1 =>
                                array (
                                    'name' => 'USERCITY',
                                    'sanitizedName' => 'USERCITY',
                                    'type' => 'NVARCHAR2',
                                    'nullable' => true,
                                    'length' => '400',
                                    'ordinalPosition' => 2,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            2 =>
                                array (
                                    'name' => 'USERSENTIMENT',
                                    'sanitizedName' => 'USERSENTIMENT',
                                    'type' => 'NVARCHAR2',
                                    'nullable' => true,
                                    'length' => '400',
                                    'ordinalPosition' => 3,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            3 =>
                                array (
                                    'name' => 'ZIPCODE',
                                    'sanitizedName' => 'ZIPCODE',
                                    'type' => 'NVARCHAR2',
                                    'nullable' => true,
                                    'length' => '400',
                                    'ordinalPosition' => 4,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            4 =>
                                array (
                                    'name' => 'SKU',
                                    'sanitizedName' => 'SKU',
                                    'type' => 'NVARCHAR2',
                                    'nullable' => true,
                                    'length' => '400',
                                    'ordinalPosition' => 5,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            5 =>
                                array (
                                    'name' => 'CREATEDAT',
                                    'sanitizedName' => 'CREATEDAT',
                                    'type' => 'NVARCHAR2',
                                    'nullable' => true,
                                    'length' => '400',
                                    'ordinalPosition' => 6,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            6 =>
                                array (
                                    'name' => 'CATEGORY',
                                    'sanitizedName' => 'CATEGORY',
                                    'type' => 'NVARCHAR2',
                                    'nullable' => true,
                                    'length' => '400',
                                    'ordinalPosition' => 7,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            7 =>
                                array (
                                    'name' => 'PRICE',
                                    'sanitizedName' => 'PRICE',
                                    'type' => 'NVARCHAR2',
                                    'nullable' => true,
                                    'length' => '400',
                                    'ordinalPosition' => 8,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            8 =>
                                array (
                                    'name' => 'COUNTY',
                                    'sanitizedName' => 'COUNTY',
                                    'type' => 'NVARCHAR2',
                                    'nullable' => true,
                                    'length' => '400',
                                    'ordinalPosition' => 9,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            9 =>
                                array (
                                    'name' => 'COUNTYCODE',
                                    'sanitizedName' => 'COUNTYCODE',
                                    'type' => 'NVARCHAR2',
                                    'nullable' => true,
                                    'length' => '400',
                                    'ordinalPosition' => 10,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            10 =>
                                array (
                                    'name' => 'USERSTATE',
                                    'sanitizedName' => 'USERSTATE',
                                    'type' => 'NVARCHAR2',
                                    'nullable' => true,
                                    'length' => '400',
                                    'ordinalPosition' => 11,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            11 =>
                                array (
                                    'name' => 'CATEGORYGROUP',
                                    'sanitizedName' => 'CATEGORYGROUP',
                                    'type' => 'NVARCHAR2',
                                    'nullable' => true,
                                    'length' => '400',
                                    'ordinalPosition' => 12,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                        ),
                ),
        );
        $this->assertEquals($expectedTables, $result['tables']);
    }

    public function testMetadataManifest(): void
    {
        $config = $this->getConfig('oracle');

        // use just 1 table
        unset($config['parameters']['tables'][0]);
        unset($config['parameters']['tables'][1]);

        $app = $this->createApplication($config);

        $this->setupTestTables();

        $result = $app->run();

        $outputManifest = json_decode(
            (string) file_get_contents($this->dataDir . '/out/tables/in.c-main.tablecolumns.csv.manifest'),
            true
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
                            'value' => '400',
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.sourceName',
                            'value' => 'USERGENDER',
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.sanitizedName',
                            'value' => 'USERGENDER',
                        ),
                    6 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => '1',
                        ),
                    7 =>
                        array (
                            'key' => 'KBC.primaryKey',
                            'value' => false,
                        ),
                    8 =>
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
                            'value' => '400',
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.sourceName',
                            'value' => 'USERCITY',
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.sanitizedName',
                            'value' => 'USERCITY',
                        ),
                    6 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => '2',
                        ),
                    7 =>
                        array (
                            'key' => 'KBC.primaryKey',
                            'value' => false,
                        ),
                    8 =>
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
                            'value' => '400',
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.sourceName',
                            'value' => 'USERSENTIMENT',
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.sanitizedName',
                            'value' => 'USERSENTIMENT',
                        ),
                    6 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => '3',
                        ),
                    7 =>
                        array (
                            'key' => 'KBC.primaryKey',
                            'value' => false,
                        ),
                    8 =>
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
                            'value' => '400',
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.sourceName',
                            'value' => 'ZIPCODE',
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.sanitizedName',
                            'value' => 'ZIPCODE',
                        ),
                    6 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => '4',
                        ),
                    7 =>
                        array (
                            'key' => 'KBC.primaryKey',
                            'value' => false,
                        ),
                    8 =>
                        array (
                            'key' => 'KBC.uniqueKey',
                            'value' => false,
                        ),
                ),
        );
        $this->assertEquals($expectedColumnMetadata, $outputManifest['column_metadata']);
    }

    public function testRunEmptyResultSet(): void
    {
        $regionsManifestFile = $this->dataDir . '/out/tables/in.c-main.regions.csv.manifest';
        $regionsDataFile = $this->dataDir . '/out/tables/in.c-main.regions.csv';
        @unlink($regionsDataFile);
        @unlink($regionsManifestFile);

        $config = $this->getConfig('oracle');
        unset($config['parameters']['tables'][0]);
        unset($config['parameters']['tables'][1]);
        unset($config['parameters']['tables'][2]);
        unset($config['parameters']['tables'][3]['table']);
        $config['parameters']['tables'][3]['query'] = 'SELECT * FROM HR.REGIONS WHERE REGION_ID > 5';

        $result = ($this->createApplication($config)->run());

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);

        $this->assertFileNotExists($regionsManifestFile);
        $this->assertFileNotExists($regionsDataFile);
    }

    public function testExtractClob(): void
    {
        $config = $this->getConfig('oracle');
        unset($config['parameters']['tables'][2]);
        unset($config['parameters']['tables'][1]);
        unset($config['parameters']['tables'][0]['query']);
        $config['parameters']['tables'][0]['id'] = 342;
        $config['parameters']['tables'][0]['name'] = 'clob_test';
        $config['parameters']['tables'][0]['table']['tableName'] = 'CLOB_TEST';
        $config['parameters']['tables'][0]['table']['schema'] = 'TESTER';
        $config['parameters']['tables'][0]['outputTable'] = 'in.c-main.clob_test';

        $result = ($this->createApplication($config))->run();
        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($this->dataDir . '/out/tables/in.c-main.clob_test.csv');
        $output = file_get_contents($this->dataDir . '/out/tables/in.c-main.clob_test.csv');
        $this->assertEquals(
            "\"hello\",\"<test>some test xml </test>\"
\"nullTest\",\"\"
\"goodbye\",\"<test>some test xml </test>\"\n",
            $output
        );

        $filenameManifest = $this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv.manifest';
        $this->assertFileExists($filenameManifest);
    }

    public function testTrailingSemiColon(): void
    {
        $regionsManifestFile = $this->dataDir . '/out/tables/in.c-main.regions.csv.manifest';
        $regionsDataFile = $this->dataDir . '/out/tables/in.c-main.regions.csv';
        @unlink($regionsDataFile);
        @unlink($regionsManifestFile);

        $config = $this->getConfig('oracle');
        unset($config['parameters']['tables'][0]);
        unset($config['parameters']['tables'][1]);
        unset($config['parameters']['tables'][2]);
        unset($config['parameters']['tables'][3]['table']);
        $config['parameters']['tables'][3]['query'] = 'SELECT * FROM HR.REGIONS;';

        $result = ($this->createApplication($config))->run();
        $this->assertEquals('success', $result['status']);
    }
}
