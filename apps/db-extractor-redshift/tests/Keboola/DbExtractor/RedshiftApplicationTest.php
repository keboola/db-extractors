<?php

namespace Keboola\DbExtractor\Tests;

use Keboola\Csv\CsvFile;
use Symfony\Component\Process\Process;
use Symfony\Component\Yaml\Yaml;

class RedshiftApplicationTest extends AbstractRedshiftTest
{
    const ROOT_PATH = __DIR__ . '/../../..';

    public function testTestConnectionAction()
    {
        $config = $this->getConfig();
        $config['action'] = 'testConnection';
        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . self::ROOT_PATH . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        var_dump($process->getErrorOutput());
        var_dump($process->getOutput());

        $this->assertEquals(0, $process->getExitCode());
        $this->assertJson($process->getOutput());
        $this->assertEquals("", $process->getErrorOutput());
    }

    public function testTestSSHConnectionAction()
    {
        $config = $this->getConfig();

        $config['action'] = 'testConnection';
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getRedshiftPrivateKey(),
                'public' => $this->getEnv('redshift', 'DB_SSH_KEY_PUBLIC')
            ],
            'user' => 'root',
            'sshHost' => 'sshproxy',
            'localPort' => '33308',
            'remoteHost' => $this->getEnv('redshift', 'DB_HOST'),
            'remotePort' => $this->getEnv('redshift', 'DB_PORT')
        ];

        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . self::ROOT_PATH . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        var_dump($process->getErrorOutput());
        var_dump($process->getOutput());

        $this->assertEquals(0, $process->getExitCode());
        $this->assertJson($process->getOutput());
        $this->assertEquals("", $process->getErrorOutput());
    }

    public function testRunAction()
    {
        $config = $this->getConfig();
        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        // run entrypoint
        $process = new Process('php ' . self::ROOT_PATH . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        var_dump($process->getErrorOutput());
        var_dump($process->getOutput());

        $this->assertEquals(0, $process->getExitCode());
        $this->assertContains(
            "Query returned empty result. Nothing was imported to [in.c-main.escapingEmpty]",
            $process->getErrorOutput()
        );

        $expectedCsvFile = $this->dataDir .  "/in/tables/escaping.csv";
        $outputCsvFile = $this->dataDir . '/out/tables/in.c-main.escaping.csv';
        $outputManifestFile = $this->dataDir . '/out/tables/in.c-main.escaping.csv.manifest';
        $manifest = Yaml::parse(file_get_contents($outputManifestFile));

        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($outputManifestFile);;
        $this->assertEquals(file_get_contents($expectedCsvFile), file_get_contents($outputCsvFile));
        $this->assertEquals('in.c-main.escaping', $manifest['destination']);
        $this->assertEquals(true, $manifest['incremental']);
        $this->assertEquals('col3', $manifest['primary_key'][0]);
    }

    public function testSSHRunAction()
    {
        $config = $this->getConfig();
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getRedshiftPrivateKey(),
                'public' => $this->getEnv('redshift', 'DB_SSH_KEY_PUBLIC')
            ],
            'user' => 'root',
            'sshHost' => 'sshproxy',
            'localPort' => '33309',
            'remoteHost' => $this->getEnv('redshift', 'DB_HOST'),
            'remotePort' => $this->getEnv('redshift', 'DB_PORT')
        ];
        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        // run entrypoint
        $process = new Process('php ' . self::ROOT_PATH . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        var_dump($process->getErrorOutput());
        var_dump($process->getOutput());

        $this->assertContains(
            "Query returned empty result. Nothing was imported to [in.c-main.escapingEmpty]",
            $process->getErrorOutput()
        );
        $this->assertEquals(0, $process->getExitCode());

        $expectedCsvFile1 = $this->dataDir .  "/in/tables/escaping.csv";
        $expectedCsvFile2 = $this->dataDir .  "/in/tables/tableColumns.csv";
        $outputCsvFile1 = $this->dataDir . '/out/tables/in.c-main.escaping.csv';
        $outputCsvFile2 = $this->dataDir . '/out/tables/in.c-main.tableColumns.csv';
        $outputManifestFile1 = $this->dataDir . '/out/tables/in.c-main.escaping.csv.manifest';
        $outputManifestFile2 = $this->dataDir . '/out/tables/in.c-main.tablecolumns.csv.manifest';
        $manifest1 = Yaml::parse(file_get_contents($outputManifestFile1));
        $manifest2 = Yaml::parse(file_get_contents($outputManifestFile2));

        $this->assertFileExists($outputCsvFile1);
        $this->assertFileExists($outputCsvFile2);
        $this->assertFileExists($outputManifestFile1);
        $this->assertFileExists($outputManifestFile2);
        $outputArr1 = iterator_to_array(new CsvFile($outputCsvFile1));
        $expectedArr1 = iterator_to_array(new CsvFile($expectedCsvFile1));
        foreach ($expectedArr1 as $row) {
            $this->assertContains($row, $outputArr1);
        }
        $outputArr2 = iterator_to_array(new CsvFile($outputCsvFile2));
        $expectedArr2 = iterator_to_array(new CsvFile($expectedCsvFile2));
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
        $this->assertEquals(["col1", "col2"], $manifest2['columns']);
        $this->assertArrayHasKey('column_metadata', $manifest2);
    }

    public function testGetTablesAction()
    {
        $config = $this->getConfig();
        $config['action'] = "getTables";
        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        // run entrypoint
        $process = new Process('php ' . self::ROOT_PATH . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        var_dump($process->getErrorOutput());
        var_dump($process->getOutput());

        $this->assertEquals(0, $process->getExitCode());
        $this->assertJson($process->getOutput());
        $this->assertEquals("", $process->getErrorOutput());
    }
}
