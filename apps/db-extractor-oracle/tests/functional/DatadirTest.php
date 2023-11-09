<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\FunctionalTests;

use Keboola\DatadirTests\DatadirTestCase;
use Keboola\DatadirTests\Exception\DatadirTestsException;
use Keboola\DbExtractor\TraitTests\CloseSshTunnelsTrait;
use Keboola\DbExtractor\TraitTests\RemoveAllTablesTrait;
use PDO;
use RuntimeException;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Process\Process;
use Throwable;

class DatadirTest extends DatadirTestCase
{
    use RemoveAllTablesTrait;
    use CloseSshTunnelsTrait;

    protected TestConnection $connection;

    protected string $testProjectDir;

    protected string $testTempDir;

    protected ?string $kbcRealuser = null;

    public function getConnection(): TestConnection
    {
        return $this->connection;
    }

    public function setKbcRealUser(?string $realUser): void
    {
        $this->kbcRealuser = $realUser;
    }

    public static function setUpBeforeClass(): void
    {
        parent::setUpBeforeClass();

        putenv('SSH_PRIVATE_KEY=' . (string) file_get_contents('/root/.ssh/id_rsa'));
        putenv('SSH_PUBLIC_KEY=' . (string) file_get_contents('/root/.ssh/id_rsa.pub'));
    }

    public function assertDirectoryContentsSame(string $expected, string $actual): void
    {
        $this->prettifyAllManifests($actual);
        parent::assertDirectoryContentsSame($expected, $actual);
    }

    protected function setUp(): void
    {
        parent::setUp();
        putenv('KBC_COMPONENT_RUN_MODE=run');

        // Clear KBC_REALUSER env
        $this->kbcRealuser = null;

        // Test dir, eg. "/code/tests/functional/full-load-ok"
        $this->testProjectDir = $this->getTestFileDir() . '/' . $this->dataName();
        $this->testTempDir = $this->temp->getTmpFolder();

        $this->connection = TestConnection::createConnection();
        $this->removeAllTables();
        $this->closeSshTunnels();

        // Load setUp.php file - used to init database state
        $setUpPhpFile = $this->testProjectDir . '/setUp.php';
        if (file_exists($setUpPhpFile)) {
            // Get callback from file and check it
            $initCallback = require $setUpPhpFile;
            if (!is_callable($initCallback)) {
                throw new RuntimeException(sprintf('File "%s" must return callback!', $setUpPhpFile));
            }

            // Invoke callback
            $initCallback($this);
        }
    }

    protected function prettifyAllManifests(string $actual): void
    {
        foreach ($this->findManifests($actual . '/tables') as $file) {
            $this->prettifyJsonFile((string) $file->getRealPath());
        }
    }

    protected function prettifyJsonFile(string $path): void
    {
        $json = (string) file_get_contents($path);
        try {
            file_put_contents($path, (string) json_encode(json_decode($json), JSON_PRETTY_PRINT));
        } catch (Throwable $e) {
            // If a problem occurs, preserve the original contents
            file_put_contents($path, $json);
        }
    }

    protected function findManifests(string $dir): Finder
    {
        $finder = new Finder();
        return $finder->files()->in($dir)->name(['~.*\.manifest~']);
    }

    protected function runScript(string $datadirPath, ?string $runId = null): Process
    {
        $fs = new Filesystem();

        $script = $this->getScript();
        if (!$fs->exists($script)) {
            throw new DatadirTestsException(sprintf(
                'Cannot open script file "%s"',
                $script,
            ));
        }

        $runCommand = [
            'php',
            $script,
        ];
        $runProcess = new Process($runCommand);
        $defaultRunId = random_int(1000, 100000) . '.' . random_int(1000, 100000) . '.' . random_int(1000, 100000);
        $environments = [
            'KBC_DATADIR' => $datadirPath,
            'KBC_RUNID' => $runId ?? $defaultRunId,
            'KBC_REALUSER' => (string) $this->kbcRealuser,
        ];
        if (getEnv('KBC_COMPONENT_RUN_MODE')) {
            $environments['KBC_COMPONENT_RUN_MODE'] = (string) getEnv('KBC_COMPONENT_RUN_MODE');
        }

        $runProcess->setEnv($environments);
        $runProcess->setTimeout(0.0);
        $runProcess->run();
        return $runProcess;
    }
}
