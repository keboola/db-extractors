<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Component\Logger;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Extractor\Oracle;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Monolog\Handler\TestHandler;
use Psr\Log\Test\TestLogger;
use Symfony\Component\Process\Process;
use SplFileInfo;

class OracleTest extends OracleBaseTest
{
    public function testCredentials(): void
    {
        $config = $this->getConfig('oracle');
        $config['action'] = 'testConnection';
        unset($config['parameters']['tables']);

        $logger = new TestLogger();
        $app = $this->createApplication($config, [], $logger);
        $result = $app->run();

        // Check host and port in log msg
        $this->assertTrue(
            $logger->hasInfoThatContains(
                'Created "test connection" configuration for "java-oracle-exporter" tool, host: "oracle", port: "1521".'
            )
        );

        // Check output
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
        $this->setupTestRowTable();

        $logger = new TestLogger();
        $app = $this->createApplication($config, [], $logger);
        $result = $app->run();

        // Check host and port in log msg
        $this->assertTrue(
            $logger->hasInfoThatContains(
                'Created "export" configuration for "java-oracle-exporter" tool, host: "oracle", port: "1521".'
            )
        );

        // Check output
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
        $this->setupTestTables();

        $logger = new TestLogger();
        $app = $this->createApplication($config, [], $logger);
        $result = $app->run();

        // Check host and port in log msg
        $this->assertTrue(
            $logger->hasInfoThatContains(
                'Created "export" configuration for "java-oracle-exporter" tool, host: "oracle", port: "1521".'
            )
        );

        // Check output
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

        $logger = new TestLogger();
        $app = $this->createApplication($config, [], $logger);
        $result = $app->run();

        // Check host and port in log msg
        $this->assertTrue(
            $logger->hasInfoThatContains(
                'Created "test connection" configuration for "java-oracle-exporter" tool, ' .
                'host: "127.0.0.1", port: "15211".'
            )
        );

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

        $logger = new TestLogger();
        $app = $this->createApplication($config, [], $logger);

        $this->setupTestTables();

        $salesCsv = new SplFileInfo($this->dataDir . '/oracle/sales.csv');
        $escapingCsv = new SplFileInfo($this->dataDir . '/oracle/headerlessEscaping.csv');

        $result = $app->run();

        // Check host and port in log msg
        $this->assertTrue(
            $logger->hasInfoThatContains(
                'Created "export" configuration for "java-oracle-exporter" tool, host: "127.0.0.1", port: "15212".'
            )
        );

        // Check output
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

    public function testExtractorGetTablesWithSchemaHR(): void
    {
        $config = $this->getConfig('oracle');
        $config['parameters']['db']['password'] = $config['parameters']['db']['#password'];
        $config['parameters']['tables'] = [];

        $tableName = 'REGIONS';
        $config['parameters']['tableListFilter'] = [
            'listColumns' => true,
            'tablesToList' => [
                [
                    'tableName' => $tableName,
                    'schema' => 'HR',
                ],
            ],
        ];

        $extractor = new Oracle(
            $config['parameters'],
            [],
            new Logger()
        );
        $extractor->testConnection(); // expect no error
        $tables = $extractor->getTables();

        $this->assertCount(1, $tables);
        $table = $tables[0];

        $this->assertArrayHasKey('name', $table);
        $this->assertSame($tableName, $table['name']);

        $this->assertArrayHasKey('schema', $table);
        $this->assertSame('HR', $table['schema']);

        $this->assertArrayHasKey('columns', $table);
        $this->assertCount(2, $table['columns']);
    }

    public function testExtractorGetTablesWithSchemaUser(): void
    {
        $config = $this->getConfig('oracle');
        $config['parameters']['db']['password'] = $config['parameters']['db']['#password'];
        $config['parameters']['tables'] = [];

        $tableName = 'REGIONS';
        $userSchema = mb_strtoupper($config['parameters']['db']['user']);
        $config['parameters']['tableListFilter'] = [
            'listColumns' => true,
            'tablesToList' => [
                [
                    'tableName' => $tableName,
                    'schema' => $userSchema,
                ],
            ],
        ];

        $extractor = new Oracle(
            $config['parameters'],
            [],
            new Logger()
        );
        $extractor->testConnection(); // expect no error
        $tables = $extractor->getTables();

        $this->assertCount(1, $tables);
        $table = $tables[0];

        $this->assertArrayHasKey('name', $table);
        $this->assertSame($tableName, $table['name']);

        $this->assertArrayHasKey('schema', $table);
        $this->assertSame($userSchema, $table['schema']);

        $this->assertArrayHasKey('columns', $table);
        $this->assertCount(1, $table['columns']);
    }

    public function testExtractorGetTablesWithSchemaBoth(): void
    {
        $config = $this->getConfig('oracle');
        $config['parameters']['db']['password'] = $config['parameters']['db']['#password'];
        $config['parameters']['tables'] = [];

        $tableName = 'REGIONS';
        $userSchema = mb_strtoupper($config['parameters']['db']['user']);
        $config['parameters']['tableListFilter'] = [
            'listColumns' => true,
            'tablesToList' => [
                [
                    'tableName' => $tableName,
                    'schema' => 'HR',
                ],
                [
                    'tableName' => $tableName,
                    'schema' => $userSchema,
                ],
            ],
        ];
        $extractor = new Oracle(
            $config['parameters'],
            [],
            new Logger()
        );
        $extractor->testConnection(); // expect no error
        $tables = $extractor->getTables();
        $this->assertCount(2, $tables);
    }

    public function testSSHTestConnectionFailed(): void
    {
        $config = $this->getConfigRow('oracle');
        $config['action'] = 'testConnection';
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

        // Create extractor: SSH tunnel is created
        $extractor = new Oracle($config['parameters'], [], new Logger());

        // Kill SSH tunnel
        $process = new Process(['sh', '-c', 'pgrep ssh | xargs -r kill']);
        $process->mustRun();

        // Test connection must fail.
        // Test whether the SSH tunnel is really used,
        // because the direct connection is also available in the test environment.
        $this->expectException(UserException::class);
        $this->expectExceptionMessage('The Network Adapter could not establish the connection');
        $extractor->testConnection();
    }

    public function testSSHRunConnectionFailed(): void
    {
        $config = $this->getConfigRow('oracle');
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
        $config['parameters']['columns'] = [];
        $config['parameters']['outputTable'] =  'output';
        $config['parameters']['primaryKey'] = [];
        $config['parameters']['retries'] = 3;

        // Create extractor: SSH tunnel is created
        $extractor = new Oracle($config['parameters'], [], new Logger());

        // Kill SSH tunnel
        $process = new Process(['sh', '-c', 'pgrep ssh | xargs -r kill']);
        $process->mustRun();

        // Export must fail
        // Test whether the SSH tunnel is really used,
        // because the direct connection is also available in the test environment.
        $this->expectException(UserException::class);
        $this->expectExceptionMessage('The Network Adapter could not establish the connection');
        $extractor->export(ExportConfig::fromArray($config['parameters']));
    }

    public function testGetTables(): void
    {
        $config = $this->getConfig('oracle');
        $config['action'] = 'getTables';

        $app = $this->createApplication($config);

        $csv1 = new SplFileInfo($this->dataDir . '/oracle/sales.csv');
        $this->createTextTable($csv1);

        $csv2 = new SplFileInfo($this->dataDir . '/oracle/escaping.csv');
        $this->createTextTable($csv2);

        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertArrayHasKey('tables', $result);
        $this->assertEquals('success', $result['status']);
        $this->assertCount(11, $result['tables']);

        $expectedTables = [
            [
                'name' => 'DEPARTMENTS',
                'schema' => 'HR',
                'columns' =>
                    [
                        [
                            'name' => 'DEPARTMENT_ID',
                            'type' => 'NUMBER',
                            'primaryKey' => true,
                        ],
                        [
                            'name' => 'DEPARTMENT_NAME',
                            'type' => 'VARCHAR2',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'MANAGER_ID',
                            'type' => 'NUMBER',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'LOCATION_ID',
                            'type' => 'NUMBER',
                            'primaryKey' => false,
                        ],
                    ],
            ],
            [
                'name' => 'EMPLOYEES',
                'schema' => 'HR',
                'columns' =>
                    [

                        [
                            'name' => 'EMPLOYEE_ID',
                            'type' => 'NUMBER',
                            'primaryKey' => true,
                        ],
                        [
                            'name' => 'FIRST_NAME',
                            'type' => 'VARCHAR2',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'LAST_NAME',
                            'type' => 'VARCHAR2',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'EMAIL',
                            'type' => 'VARCHAR2',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'PHONE_NUMBER',
                            'type' => 'VARCHAR2',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'HIRE_DATE',
                            'type' => 'DATE',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'JOB_ID',
                            'type' => 'VARCHAR2',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'SALARY',
                            'type' => 'NUMBER',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'COMMISSION_PCT',
                            'type' => 'NUMBER',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'MANAGER_ID',
                            'type' => 'NUMBER',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'DEPARTMENT_ID',
                            'type' => 'NUMBER',
                            'primaryKey' => false,
                        ],
                    ],
            ],
            [
                'name' => 'JOBS',
                'schema' => 'HR',
                'columns' =>
                    [

                        [
                            'name' => 'JOB_ID',
                            'type' => 'VARCHAR2',
                            'primaryKey' => true,
                        ],
                        [
                            'name' => 'JOB_TITLE',
                            'type' => 'VARCHAR2',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'MIN_SALARY',
                            'type' => 'NUMBER',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'MAX_SALARY',
                            'type' => 'NUMBER',
                            'primaryKey' => false,
                        ],
                    ],
            ],
            [
                'name' => 'JOB_HISTORY',
                'schema' => 'HR',
                'columns' =>
                    [

                        [
                            'name' => 'EMPLOYEE_ID',
                            'type' => 'NUMBER',
                            'primaryKey' => true,
                        ],
                        [
                            'name' => 'START_DATE',
                            'type' => 'DATE',
                            'primaryKey' => true,
                        ],
                        [
                            'name' => 'END_DATE',
                            'type' => 'DATE',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'JOB_ID',
                            'type' => 'VARCHAR2',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'DEPARTMENT_ID',
                            'type' => 'NUMBER',
                            'primaryKey' => false,
                        ],
                    ],
            ],
            [
                'name' => 'LOCATIONS',
                'schema' => 'HR',
                'columns' =>
                    [

                        [
                            'name' => 'LOCATION_ID',
                            'type' => 'NUMBER',
                            'primaryKey' => true,
                        ],
                        [
                            'name' => 'STREET_ADDRESS',
                            'type' => 'VARCHAR2',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'POSTAL_CODE',
                            'type' => 'VARCHAR2',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'CITY',
                            'type' => 'VARCHAR2',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'STATE_PROVINCE',
                            'type' => 'VARCHAR2',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'COUNTRY_ID',
                            'type' => 'CHAR',
                            'primaryKey' => false,
                        ],
                    ],
            ],
            [
                'name' => 'REGIONS',
                'schema' => 'HR',
                'columns' =>
                    [

                        [
                            'name' => 'REGION_ID',
                            'type' => 'NUMBER',
                            'primaryKey' => true,
                        ],
                        [
                            'name' => 'REGION_NAME',
                            'type' => 'VARCHAR2',
                            'primaryKey' => false,
                        ],
                    ],
            ],
            [
                'name' => 'AUTO_INCREMENT_TIMESTAMP',
                'schema' => 'TESTER',
                'columns' => [
                    [
                        'name' => 'id',
                        'type' => 'NUMBER',
                        'primaryKey' => true,
                    ],
                    [
                        'name' => 'name',
                        'type' => 'NVARCHAR2',
                        'primaryKey' => false,
                    ],
                    [
                        'name' => 'decimal',
                        'type' => 'NUMBER',
                        'primaryKey' => false,
                    ],
                    [
                        'name' => 'date',
                        'type' => 'DATE',
                        'primaryKey' => false,
                    ],
                    [
                        'name' => 'timestamp',
                        'type' => 'TIMESTAMP(6)',
                        'primaryKey' => false,
                    ],
                ],
            ],
            [
                'name' => 'CLOB_TEST',
                'schema' => 'TESTER',
                'columns' =>
                    [

                        [
                            'name' => 'ID',
                            'type' => 'VARCHAR2',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'CLOB_COL',
                            'type' => 'CLOB',
                            'primaryKey' => false,
                        ],
                    ],
            ],
            [
                'name' => 'ESCAPING',
                'schema' => 'TESTER',
                'columns' =>
                    [

                        [
                            'name' => '_funnY#-col',
                            'type' => 'NVARCHAR2',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => '_s%d-col',
                            'type' => 'NVARCHAR2',
                            'primaryKey' => false,
                        ],
                    ],
            ],
            [
                'name' => 'REGIONS',
                'schema' => 'TESTER',
                'columns' =>
                    [

                        [
                            'name' => 'REGION_ID',
                            'type' => 'NUMBER',
                            'primaryKey' => true,
                        ],
                    ],
            ],
            [
                'name' => 'SALES',
                'schema' => 'TESTER',
                'columns' =>
                    [

                        [
                            'name' => 'USERGENDER',
                            'type' => 'NVARCHAR2',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'USERCITY',
                            'type' => 'NVARCHAR2',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'USERSENTIMENT',
                            'type' => 'NVARCHAR2',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'ZIPCODE',
                            'type' => 'NVARCHAR2',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'SKU',
                            'type' => 'NVARCHAR2',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'CREATEDAT',
                            'type' => 'NVARCHAR2',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'CATEGORY',
                            'type' => 'NVARCHAR2',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'PRICE',
                            'type' => 'NVARCHAR2',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'COUNTY',
                            'type' => 'NVARCHAR2',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'COUNTYCODE',
                            'type' => 'NVARCHAR2',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'USERSTATE',
                            'type' => 'NVARCHAR2',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'CATEGORYGROUP',
                            'type' => 'NVARCHAR2',
                            'primaryKey' => false,
                        ],
                    ],
            ],
        ];
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

        $app->run();

        $outputManifest = json_decode(
            (string) file_get_contents($this->dataDir . '/out/tables/in.c-main.tablecolumns.csv.manifest'),
            true
        );

        $this->assertArrayHasKey('destination', $outputManifest);
        $this->assertArrayHasKey('incremental', $outputManifest);
        $this->assertArrayHasKey('metadata', $outputManifest);

        $expectedMetadata = [
            [
                'key' => 'KBC.name',
                'value' => 'SALES',
            ],
            [
                'key' => 'KBC.sanitizedName',
                'value' => 'SALES',
            ],
            [
                'key' => 'KBC.schema',
                'value' => 'TESTER',
            ],
            [
                'key' => 'KBC.catalog',
                'value' => 'USERS',
            ],
            [
                'key' => 'KBC.tablespaceName',
                'value' => 'USERS',
            ],
            [
                'key' => 'KBC.owner',
                'value' => 'TESTER',
            ],
        ];

        $this->assertEquals($expectedMetadata, $outputManifest['metadata']);
        $this->assertArrayHasKey('column_metadata', $outputManifest);
        $this->assertCount(4, $outputManifest['column_metadata']);

        $expectedColumnMetadata = [
            'USERGENDER' =>
                [

                    [
                        'key' => 'KBC.datatype.type',
                        'value' => 'NVARCHAR2',
                    ],
                    [
                        'key' => 'KBC.datatype.nullable',
                        'value' => true,
                    ],
                    [
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'STRING',
                    ],
                    [
                        'key' => 'KBC.datatype.length',
                        'value' => '400',
                    ],
                    [
                        'key' => 'KBC.sourceName',
                        'value' => 'USERGENDER',
                    ],
                    [
                        'key' => 'KBC.sanitizedName',
                        'value' => 'USERGENDER',
                    ],
                    [
                        'key' => 'KBC.primaryKey',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.uniqueKey',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.ordinalPosition',
                        'value' => '1',
                    ],
                ],
            'USERCITY' =>
                [

                    [
                        'key' => 'KBC.datatype.type',
                        'value' => 'NVARCHAR2',
                    ],
                    [
                        'key' => 'KBC.datatype.nullable',
                        'value' => true,
                    ],
                    [
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'STRING',
                    ],
                    [
                        'key' => 'KBC.datatype.length',
                        'value' => '400',
                    ],
                    [
                        'key' => 'KBC.sourceName',
                        'value' => 'USERCITY',
                    ],
                    [
                        'key' => 'KBC.sanitizedName',
                        'value' => 'USERCITY',
                    ],
                    [
                        'key' => 'KBC.primaryKey',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.uniqueKey',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.ordinalPosition',
                        'value' => '2',
                    ],
                ],
            'USERSENTIMENT' =>
                [

                    [
                        'key' => 'KBC.datatype.type',
                        'value' => 'NVARCHAR2',
                    ],
                    [
                        'key' => 'KBC.datatype.nullable',
                        'value' => true,
                    ],
                    [
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'STRING',
                    ],
                    [
                        'key' => 'KBC.datatype.length',
                        'value' => '400',
                    ],
                    [
                        'key' => 'KBC.sourceName',
                        'value' => 'USERSENTIMENT',
                    ],
                    [
                        'key' => 'KBC.sanitizedName',
                        'value' => 'USERSENTIMENT',
                    ],
                    [
                        'key' => 'KBC.primaryKey',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.uniqueKey',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.ordinalPosition',
                        'value' => '3',
                    ],
                ],
            'ZIPCODE' =>
                [

                    [
                        'key' => 'KBC.datatype.type',
                        'value' => 'NVARCHAR2',
                    ],
                    [
                        'key' => 'KBC.datatype.nullable',
                        'value' => true,
                    ],
                    [
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'STRING',
                    ],
                    [
                        'key' => 'KBC.datatype.length',
                        'value' => '400',
                    ],
                    [
                        'key' => 'KBC.sourceName',
                        'value' => 'ZIPCODE',
                    ],
                    [
                        'key' => 'KBC.sanitizedName',
                        'value' => 'ZIPCODE',
                    ],
                    [
                        'key' => 'KBC.primaryKey',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.uniqueKey',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.ordinalPosition',
                        'value' => '4',
                    ],
                ],
        ];
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

        $this->assertFileDoesNotExist($regionsManifestFile);
        $this->assertFileDoesNotExist($regionsDataFile);
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

    public function testTnsnames(): void
    {
        $tnsnameTemplate = <<<EOL
XE =
  (DESCRIPTION =
    (ADDRESS = (PROTOCOL = tcp)(HOST=%s)(PORT = %s))
    (CONNECT_DATA =
      (SID=%s)
      (SERVICE_NAME = XE)
    )
  )
EOL;
        $config = $this->getConfigRow('oracle');
        $tnsnameContent = sprintf(
            $tnsnameTemplate,
            $config['parameters']['db']['host'],
            $config['parameters']['db']['port'],
            $config['parameters']['db']['database']
        );
        $config['parameters']['db']['tnsnames'] = $tnsnameContent;
        unset($config['parameters']['db']['host'], $config['parameters']['db']['port']);

        $this->createApplication($config)->run();

        $this->assertFileExists($this->dataDir . '/tnsnames.ora');

        $savedTnsname = file_get_contents($this->dataDir . '/tnsnames.ora');
        $this->assertEquals($tnsnameContent, $savedTnsname);
    }

    public function testTnsnamesMissingServiceName(): void
    {
        $tnsnameTemplate = <<<EOL
MYTNSNAME =
  (DESCRIPTION =
    (ADDRESS = (PROTOCOL = tcp)(HOST=%s)(PORT = %s))
    (CONNECT_DATA =
      (SID=%s)
    )
  )
EOL;
        $config = $this->getConfigRow('oracle');
        $tnsnameContent = sprintf(
            $tnsnameTemplate,
            $config['parameters']['db']['host'],
            $config['parameters']['db']['port'],
            $config['parameters']['db']['database']
        );
        $config['parameters']['db']['tnsnames'] = $tnsnameContent;
        unset($config['parameters']['db']['host'], $config['parameters']['db']['port']);

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('DB query failed: Missing "SERVICE_NAME" in the tnsnames.');
        $this->createApplication($config)->run();
    }
}
