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

    /**
     * @dataProvider configTypesProvider
     */
    public function testRunAction($configType)
    {
        $outputCsvFile = $this->dataDir . '/out/tables/in.c-main.sales.csv';
        $outputCsvFile2 = $this->dataDir . '/out/tables/in.c-main.escaping.csv';

        @unlink($outputCsvFile);
        @unlink($outputCsvFile2);

        @unlink($this->dataDir . '/config.json');
        @unlink($this->dataDir . '/config.yml');

        $config = $this->getConfig(self::DRIVER, $configType);
        if ($configType === 'json') {
            file_put_contents($this->dataDir . '/config.json', json_encode($config));
        } else {
            file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));
        }

        $csv1 = new CsvFile($this->dataDir . '/mysql/sales.csv');
        $this->createTextTable($csv1);

        $csv2 = new CsvFile($this->dataDir . '/mysql/escaping.csv');
        $this->createTextTable($csv2);

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        echo $process->getErrorOutput();
        echo $process->getOutput();

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
        $table = $config['parameters']['tables'][2]['table'];
        unset($config['parameters']['tables'][0]);
        unset($config['parameters']['tables'][1]);

        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        // try exporting before the table exists

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->start();

        // Drop the table if it exists
        $this->pdo->exec(sprintf("DROP TABLE IF EXISTS `%s`.`%s`", $table['schema'], $table['tableName']));

        $tableCreated = false;
        while ($process->isRunning()) {
            sleep(5);
            if (!$tableCreated) {
                $csv1 = new CsvFile($this->dataDir . '/mysql/sales.csv');
                $this->createTextTable($csv1, $table['tableName']);
                $tableCreated = true;
            }
        }

        // check that it had to retry at least 2x
        $this->assertContains('[2x]', $process->getOutput());

        $expectedOutput = new CsvFile($this->dataDir . '/mysql/tableColumns.csv');

        $this->assertEquals(0, $process->getExitCode());
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($this->dataDir . '/out/tables/in.c-main.tablecolumns.csv.manifest');
        $this->assertFileEquals((string) $expectedOutput, $outputCsvFile);
        $this->assertFileExists($outputCsvFile);
    }

    public function testRunConfigRow()
    {
        $outputCsvFile = $this->dataDir . '/out/tables/in.c-main.escaping.csv';
        @unlink($outputCsvFile);

        $config = $this->getConfigRow(self::DRIVER);

        @unlink($this->dataDir . '/config.yml');
        @unlink($this->dataDir . '/config.json');

        file_put_contents($this->dataDir . '/config.json', json_encode($config));

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $expectedOutput = new CsvFile($this->dataDir . '/mysql/escaping.csv');

        $this->assertEquals(0, $process->getExitCode());
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($this->dataDir . '/out/tables/in.c-main.tablecolumns.csv.manifest');
        $this->assertFileEquals((string) $expectedOutput, $outputCsvFile);
        $this->assertFileExists($outputCsvFile);
    }
}
