<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Exception\UserException;
use Symfony\Component\Process\Process;

class ApplicationTest extends OracleBaseTest
{
    /** @var string */
    protected $rootPath = __DIR__ . '/../../..';

    public function testTestConnectionAction(): void
    {
        $config = $this->getConfig('oracle');
        $config['action'] = 'testConnection';
        $this->putConfig($config);

        $process = Process::fromShellCommandline('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals('', $process->getErrorOutput());
        $this->assertJson($process->getOutput());
    }

    public function testRunAction(): void
    {
        $outputCsvFile1 = $this->dataDir . '/out/tables/in.c-main.sales.csv';
        $outputCsvFile2 = $this->dataDir . '/out/tables/in.c-main.escaping.csv';
        $outputCsvFile3 = $this->dataDir . '/out/tables/in.c-main.tablecolumns.csv';
        $manifestFile1 = $this->dataDir . '/out/tables/in.c-main.sales.csv.manifest';
        $manifestFile2 = $this->dataDir . '/out/tables/in.c-main.escaping.csv.manifest';
        $manifestFile3 = $this->dataDir . '/out/tables/in.c-main.tablecolumns.csv.manifest';

        @unlink($outputCsvFile1);
        @unlink($outputCsvFile2);
        @unlink($outputCsvFile3);
        @unlink($manifestFile1);
        @unlink($manifestFile2);
        @unlink($manifestFile3);

        $expectedCsv1 = new CsvFile($this->dataDir . '/oracle/sales.csv');
        $expectedCsv1 = iterator_to_array($expectedCsv1);

        $expectedCsv2 = new CsvFile($this->dataDir . '/oracle/escaping.csv');
        $expectedCsv2 = iterator_to_array($expectedCsv2);
        array_shift($expectedCsv2);
        $expectedCsv3 = new CsvFile($this->dataDir . '/oracle/tableColumns.csv');
        $expectedCsv3 = iterator_to_array($expectedCsv3);
        array_shift($expectedCsv3);

        $config = $this->getConfig('oracle');
        $this->putConfig($config);

        $this->setupTestTables();

        $process = Process::fromShellCommandline('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();

        $outputCsvData1 = iterator_to_array(new CsvFile($outputCsvFile1));
        $outputCsvData2 = iterator_to_array(new CsvFile($outputCsvFile2));
        $outputCsvData3 = iterator_to_array(new CsvFile($outputCsvFile2));

        $this->assertFileExists($outputCsvFile1);
        $this->assertEquals(ksort($expectedCsv1), ksort($outputCsvData1));
        $this->assertFileExists($outputCsvFile2);
        $this->assertEquals(ksort($expectedCsv2), ksort($outputCsvData2));
        $this->assertFileExists($outputCsvFile3);
        $this->assertEquals(ksort($expectedCsv3), ksort($outputCsvData3));
        $this->assertFileExists($manifestFile1);
        $this->assertFileExists($manifestFile2);
        $this->assertFileExists($manifestFile3);
    }

    public function testRunActionSshTunnel(): void
    {
        $outputCsvFile1 = $this->dataDir . '/out/tables/in.c-main.sales.csv';
        $outputCsvFile2 = $this->dataDir . '/out/tables/in.c-main.escaping.csv';
        $outputCsvFile3 = $this->dataDir . '/out/tables/in.c-main.tablecolumns.csv';
        $manifestFile1 = $this->dataDir . '/out/tables/in.c-main.sales.csv.manifest';
        $manifestFile2 = $this->dataDir . '/out/tables/in.c-main.escaping.csv.manifest';
        $manifestFile3 = $this->dataDir . '/out/tables/in.c-main.tablecolumns.csv.manifest';

        @unlink($outputCsvFile1);
        @unlink($outputCsvFile2);
        @unlink($outputCsvFile3);
        @unlink($manifestFile1);
        @unlink($manifestFile2);
        @unlink($manifestFile3);

        $expectedCsv1 = new CsvFile($this->dataDir . '/oracle/sales.csv');
        $expectedCsv1 = iterator_to_array($expectedCsv1);

        $expectedCsv2 = new CsvFile($this->dataDir . '/oracle/escaping.csv');
        $expectedCsv2 = iterator_to_array($expectedCsv2);
        array_shift($expectedCsv2);
        $expectedCsv3 = new CsvFile($this->dataDir . '/oracle/tableColumns.csv');
        $expectedCsv3 = iterator_to_array($expectedCsv3);
        array_shift($expectedCsv3);

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
            'localPort' => '15213',
        ];
        $this->putConfig($config);
        $this->setupTestTables();

        $process = Process::fromShellCommandline('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();

        $outputCsvData1 = iterator_to_array(new CsvFile($outputCsvFile1));
        $outputCsvData2 = iterator_to_array(new CsvFile($outputCsvFile2));
        $outputCsvData3 = iterator_to_array(new CsvFile($outputCsvFile3));

        $this->assertFileExists($outputCsvFile1);
        $this->assertEquals(ksort($expectedCsv1), ksort($outputCsvData1));
        $this->assertFileExists($outputCsvFile2);
        $this->assertEquals(ksort($expectedCsv2), ksort($outputCsvData2));
        $this->assertFileExists($outputCsvFile3);
        $this->assertEquals(ksort($expectedCsv3), ksort($outputCsvData3));
        $this->assertFileExists($manifestFile1);
        $this->assertFileExists($manifestFile2);
        $this->assertFileExists($manifestFile3);
    }

    /**
     * @dataProvider configTypesProvider
     */
    public function testGetTablesAction(string $configType): void
    {
        $config = $this->getConfig('oracle');
        $config['action'] = 'getTables';
        $this->putConfig($config);

        $process = Process::fromShellCommandline('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals('', $process->getErrorOutput());
        $this->assertJson($process->getOutput());
    }

    /**
     * @dataProvider configTypesProvider
     */
    public function testGetTablesNoColumns(string $configType): void
    {
        $config = $this->getConfig('oracle');
        $config['action'] = 'getTables';
        $config['parameters']['tableListFilter'] = [
            'listColumns' => false,
        ];
        $this->putConfig($config);
        $process = Process::fromShellCommandline('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();

        $data = json_decode($process->getOutput(), true);
        self::assertCount(10, $data['tables']);
        self::assertArrayNotHasKey('columns', $data['tables'][0]);
        self::assertEquals(0, $process->getExitCode());
        self::assertEquals('', $process->getErrorOutput());
    }

    public function testGetTablesOneTableNoColumns(): void
    {
        $config = $this->getConfig('oracle');
        $config['action'] = 'getTables';
        unset($config['parameters']['tables']);
        $config['parameters']['tableListFilter'] = [
            'listColumns' => false,
            'tablesToList' => [[
                'tableName' => 'REGIONS',
                'schema' => 'HR',
            ]],
        ];
        $this->putConfig($config);
        $process = Process::fromShellCommandline('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();

        $data = json_decode($process->getOutput(), true);
        self::assertCount(1, $data['tables']);
        self::assertEquals('REGIONS', $data['tables'][0]['name']);
        self::assertArrayNotHasKey('columns', $data['tables'][0]);
        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals('', $process->getErrorOutput());
    }

    public function testRunError(): void
    {
        $config = $this->getConfig('oracle');
        unset($config['parameters']['tables'][0]);
        unset($config['parameters']['tables'][1]);
        unset($config['parameters']['tables'][2]);
        unset($config['parameters']['tables'][3]['table']);
        $config['parameters']['tables'][3]['query'] = 'SELECT SOMETHING ORDER BY INVALID FROM "invalid"."escaping"';
        $this->putConfig($config);

        $process = Process::fromShellCommandline('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(1, $process->getExitCode());
        $this->assertStringContainsString('Export process failed:', $process->getErrorOutput());
        // verify that it retries 5 times
        $this->assertStringContainsString('[4x]', $process->getOutput());
    }

    private function putConfig(array $config): void
    {
        @unlink($this->dataDir . '/config.json');
        file_put_contents($this->dataDir . '/config.json', json_encode($config));
    }
}
