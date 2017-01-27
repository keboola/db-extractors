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

        var_dump($process->getErrorOutput());
        var_dump($process->getOutput());
//        die;

        $this->assertEquals(0, $process->getExitCode());
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($this->dataDir . '/out/tables/in.c-main.sales.csv.manifest');
        $this->assertFileEquals((string) $csv1, $outputCsvFile);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($this->dataDir . '/out/tables/in.c-main.escaping.csv.manifest');
        $this->assertFileEquals((string) $csv2, $outputCsvFile2);
    }
}
