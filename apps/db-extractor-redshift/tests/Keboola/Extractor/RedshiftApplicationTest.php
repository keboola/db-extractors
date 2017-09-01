<?php

namespace Keboola\DbExtractor;

use Symfony\Component\Process\Process;
use Symfony\Component\Yaml\Yaml;

class RedshiftApplicationTest extends AbstractRedshiftTest
{
    public function testTestConnectionAction()
    {
        $config = $this->getConfig();
        $config['action'] = 'testConnection';
        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . ROOT_PATH . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

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
                '#private' => $this->getEnv('redshift', 'DB_SSH_KEY_PRIVATE'),
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

        $process = new Process('php ' . ROOT_PATH . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();
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
        $process = new Process('php ' . ROOT_PATH . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        var_dump($process->getErrorOutput());

        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals("", $process->getErrorOutput());

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
                '#private' => $this->getEnv('redshift', 'DB_SSH_KEY_PRIVATE'),
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
        $process = new Process('php ' . ROOT_PATH . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals("", $process->getErrorOutput());
        $this->assertEquals(0, $process->getExitCode());

        $expectedCsvFile1 = $this->dataDir .  "/in/tables/escaping.csv";
        $expectedCsvFile2 = $this->dataDir .  "/in/tables/tableColumns.csv";
        $outputCsvFile1 = $this->dataDir . '/out/tables/in.c-main.escaping.csv';
        $outputCsvFile2 = $this->dataDir . '/out/tables/in.c-main.tableColumns.csv';
        $outputManifestFile1 = $this->dataDir . '/out/tables/in.c-main.escaping.csv.manifest';
        $outputManifestFile2 = $this->dataDir . '/out/tables/in.c-main.tableColumns.csv.manifest';
        $manifest1 = Yaml::parse(file_get_contents($outputManifestFile1));
        $manifest2 = Yaml::parse(file_get_contents($outputManifestFile2));

        $this->assertFileExists($outputCsvFile1);
        $this->assertFileExists($outputCsvFile2);
        $this->assertFileExists($outputManifestFile1);
        $this->assertFileExists($outputManifestFile2);
        $this->assertEquals(file_get_contents($expectedCsvFile1), file_get_contents($outputCsvFile1));
        $this->assertEquals(file_get_contents($expectedCsvFile2), file_get_contents($outputCsvFile2));
        $this->assertEquals('in.c-main.escaping', $manifest1['destination']);
        $this->assertEquals(true, $manifest1['incremental']);
        $this->assertEquals('col3', $manifest1['primary_key'][0]);

        $this->assertEquals('in.c-main.tableColumns', $manifest2['destination']);
        $this->assertEquals(false, $manifest2['incremental']);
        $this->assertArrayHasKey('metadata', $manifest2);
        $this->assertArrayHasKey('column_metadata', $manifest2);
    }

    public function testGetTablesAction()
    {
        $config = $this->getConfig();
        $config['action'] = "getTables";
        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        // run entrypoint
        $process = new Process('php ' . ROOT_PATH . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertJson($process->getOutput());
        $this->assertEquals("", $process->getErrorOutput());
    }
}