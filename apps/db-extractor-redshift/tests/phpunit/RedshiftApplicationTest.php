<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Csv\CsvReader;
use Keboola\DbExtractor\TraitTests\CloseSshTunnelsTrait;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Process\Process;

class RedshiftApplicationTest extends AbstractRedshiftTest
{
    use CloseSshTunnelsTrait;

    protected const ROOT_PATH = '/code/src';

    public function setUp(): void
    {
        $this->closeSshTunnels();
        parent::setUp();
    }

    private function prepareConfig(array $config): void
    {
        @unlink($this->dataDir . '/config.json');
        file_put_contents($this->dataDir . '/config.json', json_encode($config));
    }

    public function testTestConnectionAction(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['action'] = 'testConnection';
        $this->prepareConfig($config);

        $process = Process::fromShellCommandline('php ' . self::ROOT_PATH . '/run.php');
        $process->setEnv(['KBC_DATADIR' => $this->dataDir]);
        $process->setTimeout(300);
        $process->run();
        $this->assertEquals(0, $process->getExitCode(), $process->getErrorOutput() . $process->getOutput());
        $this->assertJson($process->getOutput());
        $this->assertEquals('', $process->getErrorOutput());
    }

    public function testTestSSHConnectionAction(): void
    {
        $config = $this->getConfig();

        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getPrivateKey(),
                'public' => $this->getPublicKey(),
            ],
            'user' => 'root',
            'sshHost' => 'sshproxy',
            'localPort' => '33308',
        ];

        $this->prepareConfig($config);

        $process = Process::fromShellCommandline('php ' . self::ROOT_PATH . '/run.php');
        $process->setEnv(['KBC_DATADIR' => $this->dataDir]);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode(), $process->getErrorOutput() . $process->getOutput());
        $this->assertStringContainsString(
            'Creating SSH tunnel to \'sshproxy\' on local port \'33308\'',
            $process->getOutput()
        );
        $this->assertStringContainsString('port=33308;host=127.0.0.1', $process->getOutput());
    }

    public function testRunAction(): void
    {
        $config = $this->getConfig(self::DRIVER);

        $this->prepareConfig($config);

        // run entrypoint
        $process = Process::fromShellCommandline('php ' . self::ROOT_PATH . '/run.php');
        $process->setEnv(['KBC_DATADIR' => $this->dataDir]);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertStringContainsString(
            'Query returned empty result. Nothing was imported to [in.c-main.escapingEmpty]',
            $process->getErrorOutput()
        );

        $expectedCsvFile = $this->dataDir .  '/in/tables/escaping.csv';
        $outputCsvFile = $this->dataDir . '/out/tables/in.c-main.escaping.csv';
        $outputManifestFile = $this->dataDir . '/out/tables/in.c-main.escaping.csv.manifest';
        $manifest = json_decode((string) file_get_contents($outputManifestFile), true);

        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($outputManifestFile);
        $this->assertEquals(file_get_contents($expectedCsvFile), file_get_contents($outputCsvFile));
        $this->assertEquals('in.c-main.escaping', $manifest['destination']);
        $this->assertEquals(true, $manifest['incremental']);
        $this->assertEquals('col3', $manifest['primary_key'][0]);
    }

    public function testSSHRunAction(): void
    {
        $config = $this->getConfig();
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getPrivateKey(),
                'public' => $this->getPublicKey(),
            ],
            'user' => 'root',
            'sshHost' => 'sshproxy',
            'localPort' => '33309',
            'remoteHost' => $this->getEnv('redshift', 'DB_HOST'),
            'remotePort' => $this->getEnv('redshift', 'DB_PORT'),
        ];
        $this->prepareConfig($config);

        // run entrypoint
        $process = Process::fromShellCommandline('php ' . self::ROOT_PATH . '/run.php');
        $process->setEnv(['KBC_DATADIR' => $this->dataDir]);
        $process->setTimeout(300);
        $process->run();

        $this->assertStringContainsString(
            'Query returned empty result. Nothing was imported to [in.c-main.escapingEmpty]',
            $process->getErrorOutput()
        );
        $this->assertEquals(0, $process->getExitCode());

        $expectedCsvFile1 = $this->dataDir .  '/in/tables/escaping.csv';
        $expectedCsvFile2 = $this->dataDir .  '/in/tables/tableColumns.csv';
        $outputCsvFile1 = $this->dataDir . '/out/tables/in.c-main.escaping.csv';
        $outputCsvFile2 = $this->dataDir . '/out/tables/in.c-main.tablecolumns.csv';
        $outputManifestFile1 = $this->dataDir . '/out/tables/in.c-main.escaping.csv.manifest';
        $outputManifestFile2 = $this->dataDir . '/out/tables/in.c-main.tablecolumns.csv.manifest';
        $manifest1 = json_decode((string) file_get_contents($outputManifestFile1), true);
        $manifest2 = json_decode((string) file_get_contents($outputManifestFile2), true);

        $this->assertFileExists($outputCsvFile1);
        $this->assertFileExists($outputCsvFile2);
        $this->assertFileExists($outputManifestFile1);
        $this->assertFileExists($outputManifestFile2);
        $outputArr1 = iterator_to_array(new CsvReader($outputCsvFile1));
        $expectedArr1 = iterator_to_array(new CsvReader($expectedCsvFile1));
        foreach ($expectedArr1 as $row) {
            $this->assertContains($row, $outputArr1);
        }
        $outputArr2 = iterator_to_array(new CsvReader($outputCsvFile2));
        $expectedArr2 = iterator_to_array(new CsvReader($expectedCsvFile2));
        // simple queries don't have headers
        array_shift($expectedArr2);

        foreach ($expectedArr2 as $row) {
            $this->assertContains($row, $outputArr2);
        }

        $this->assertEquals('in.c-main.escaping', $manifest1['destination']);
        $this->assertEquals(true, $manifest1['incremental']);
        $this->assertEquals('col3', $manifest1['primary_key'][0]);

        $this->assertEquals('in.c-main.tableColumns', $manifest2['destination']);
        $this->assertEquals(false, $manifest2['incremental']);
        $this->assertArrayHasKey('metadata', $manifest2);
        $this->assertArrayHasKey('columns', $manifest2);
        $this->assertEquals(['col1', 'col2'], $manifest2['columns']);
        $this->assertArrayHasKey('column_metadata', $manifest2);
    }

    public function testGetTablesAction(): void
    {
        $config = $this->getConfig();
        $config['action'] = 'getTables';

        $this->prepareConfig($config);

        // run entrypoint
        $process = Process::fromShellCommandline('php ' . self::ROOT_PATH . '/run.php');
        $process->setEnv(['KBC_DATADIR' => $this->dataDir]);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertJson($process->getOutput());
        $this->assertEquals('', $process->getErrorOutput());
    }

    public function testRunIncrementalFetching(): void
    {
        $config = $this->getConfigRow();
        $config['parameters']['incrementalFetchingColumn'] = '_weird-i-d';
        $config['parameters']['table']['tableName'] = 'auto_increment_autoincrement';
        $config['parameters']['outputTable'] = 'in.c-main.auto-increment-autoincrement';
        $config['parameters']['columns'] = [];
        $this->createAutoIncrementAndTimestampTable($config);

        @unlink($this->dataDir . '/config.json');

        $inputStateFile = $this->dataDir . '/in/state.json';

        $fs = new Filesystem();
        if (!$fs->exists($inputStateFile)) {
            $fs->mkdir($this->dataDir . '/in');
            $fs->touch($inputStateFile);
        }
        $outputStateFile = $this->dataDir . '/out/state.json';
        // unset the state file
        @unlink($outputStateFile);
        @unlink($inputStateFile);

        file_put_contents($this->dataDir . '/config.json', json_encode($config));

        $process = Process::fromShellCommandline('php ' . self::ROOT_PATH . '/run.php');
        $process->setEnv(['KBC_DATADIR' => $this->dataDir]);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertFileExists($outputStateFile);
        $this->assertEquals(['lastFetchedRow' => '2'], json_decode((string) file_get_contents($outputStateFile), true));

        // add a couple rows
        $this->insertRowToTable($config, ['weird-Name' => 'charles']);

        // copy state to input state file
        file_put_contents($inputStateFile, file_get_contents($outputStateFile));

        // run the config again
        $process = Process::fromShellCommandline('php ' . self::ROOT_PATH . '/run.php');
        $process->setEnv(['KBC_DATADIR' => $this->dataDir]);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals(['lastFetchedRow' => '3'], json_decode((string) file_get_contents($outputStateFile), true));
    }
}
