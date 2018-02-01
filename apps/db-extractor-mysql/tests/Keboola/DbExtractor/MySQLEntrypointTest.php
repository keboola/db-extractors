<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 11/11/16
 * Time: 15:51
 */

namespace Keboola\DbExtractor\Tests;

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

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

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

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
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
                '#private' => $this->getPrivateKey('mysql'),
                'public' => $this->getEnv('mysql', 'DB_SSH_KEY_PUBLIC')
            ],
            'user' => 'root',
            'sshHost' => 'sshproxy',
            'remoteHost' => 'mysql',
            'remotePort' => '3306',
            'localPort' => '15211',
        ];
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
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

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertJson($process->getOutput());
        $this->assertEquals("", $process->getErrorOutput());
    }

    public function testTableColumnsQuery()
    {
        $outputCsvFile = $this->dataDir . '/out/tables/in.c-main.tablecolumns.csv';

        @unlink($outputCsvFile);

        $config = $this->getConfig();
        unset($config['parameters']['tables'][0]);
        unset($config['parameters']['tables'][1]);
        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $csv1 = new CsvFile($this->dataDir . '/mysql/sales.csv');
        $this->createTextTable($csv1);

        $expectedOutput = new CsvFile($this->dataDir . '/mysql/tableColumns.csv');

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($this->dataDir . '/out/tables/in.c-main.tablecolumns.csv.manifest');
        $this->assertFileEquals((string) $expectedOutput, $outputCsvFile);
        $this->assertFileExists($outputCsvFile);
    }

    public function testRetries()
    {
        $outputCsvFile = $this->dataDir . '/out/tables/in.c-main.tablecolumns.csv';

        @unlink($outputCsvFile);

        $config = $this->getConfig();
        $config['parameters']['tables'][2]['table']['tableName'] = 'late_table';
        unset($config['parameters']['tables'][0]);
        unset($config['parameters']['tables'][1]);

        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        // try exporting before the table exists

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->start();

        $this->pdo->exec("DROP TABLE IF EXISTS `late_table`");
        $tableCreated = false;
        echo "\n Table Not Created\n";
        while ($process->isRunning()) {
            sleep(5);
            if (!$tableCreated) {
                $csv1 = new CsvFile($this->dataDir . '/mysql/sales.csv');
                $this->createTextTable($csv1, 'late_table');
                $tableCreated = true;
            }
        }

        // check that it had to retry at least 2x
        $this->assertContains('[2x]', $process->getOutput());

        $expectedOutput = new CsvFile($this->dataDir . '/mysql/tableColumns.csv');
        // now create the table

        $this->assertEquals(0, $process->getExitCode());
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($this->dataDir . '/out/tables/in.c-main.tablecolumns.csv.manifest');
        $this->assertFileEquals((string) $expectedOutput, $outputCsvFile);
        $this->assertFileExists($outputCsvFile);
    }
}
