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