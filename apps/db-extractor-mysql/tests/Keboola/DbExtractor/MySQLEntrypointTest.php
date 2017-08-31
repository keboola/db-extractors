<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 11/11/16
 * Time: 15:51
 */

namespace Keboola\DbExtractor;

use Keboola\Csv\CsvFile;
use Symfony\Component\Process\Process;
use Symfony\Component\Yaml\Yaml;

class MySQLEntrypointTest extends AbstractMySQLTest
{
    public function testRunAction()
    {
        $outputCsvFile = $this->dataDir . '/out/tables/in.c-main.sales.csv';
        $outputCsvFile2 = $this->dataDir . '/out/tables/in.c-main.escaping.csv';

        @unlink($outputCsvFile);
        @unlink($outputCsvFile2);

        $config = $this->getConfig();
        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $csv1 = new CsvFile($this->dataDir . '/mysql/sales.csv');
        $this->createTextTable($csv1);

        $csv2 = new CsvFile($this->dataDir . '/mysql/escaping.csv');
        $this->createTextTable($csv2);

        // run entrypoint
        $process = new Process('php ' . ROOT_PATH . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();
//        die;

        var_dump($process->getErrorOutput());
        var_dump($process->getOutput());
        $this->assertEquals(0, $process->getExitCode());
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($this->dataDir . '/out/tables/in.c-main.sales.csv.manifest');
        $this->assertFileEquals((string) $csv1, $outputCsvFile);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($this->dataDir . '/out/tables/in.c-main.escaping.csv.manifest');
        $this->assertFileEquals((string) $csv2, $outputCsvFile2);
    }

    public function testTestConnectionAction()
    {
        $config = $this->getConfig();
        @unlink($this->dataDir . '/config.yml');
        $config['action'] = 'testConnection';
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . ROOT_PATH . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();
        $this->assertJson($process->getOutput());
        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals("", $process->getErrorOutput());
    }

    public function testTestConnectionActionWithSSH()
    {
        $config = $this->getConfig();
        @unlink($this->dataDir . '/config.yml');
        $config['action'] = 'testConnection';
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getEnv('mysql', 'DB_SSH_KEY_PRIVATE'),
                'public' => $this->getEnv('mysql', 'DB_SSH_KEY_PUBLIC')
            ],
            'user' => 'root',
            'sshHost' => 'sshproxy',
            'remoteHost' => 'mysql',
            'remotePort' => '3306',
            'localPort' => '15211',
        ];
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . ROOT_PATH . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();
        $this->assertJson($process->getOutput());
        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals("", $process->getErrorOutput());
    }

    public function testGetTablesAction()
    {
        $config = $this->getConfig();
        $config['action'] = "getTables";
        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        // run entrypoint
        $process = new Process('php ' . ROOT_PATH . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertJson($process->getOutput());
        $this->assertEquals("", $process->getErrorOutput());
    }

    public function testTableColumnsQuery()
    {
        $outputCsvFile = $this->dataDir . '/out/tables/in.c-main.tableColumns.csv';

        @unlink($outputCsvFile);

        $config = $this->getConfig();
        unset($config['tables'][0]);
        unset($config['tables'][1]);
        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $csv1 = new CsvFile($this->dataDir . '/mysql/sales.csv');
        $this->createTextTable($csv1);

        $expectedOutput = new CsvFile($this->dataDir . '/mysql/tableColumns.csv');

        // run entrypoint
        $process = new Process('php ' . ROOT_PATH . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($this->dataDir . '/out/tables/in.c-main.tableColumns.csv.manifest');
        $this->assertFileEquals((string) $expectedOutput, $outputCsvFile);
        $this->assertFileExists($outputCsvFile);
    }
}
