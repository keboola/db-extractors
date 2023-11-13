<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Component\JsonHelper;
use Keboola\DbExtractor\FunctionalTests\PdoTestConnection;
use Keboola\DbExtractor\MySQLApplication;
use Keboola\DbExtractor\Tests\Traits\ConfigTrait;
use Keboola\DbExtractor\TraitTests\RemoveAllTablesTrait;
use PDO;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

class PerformanceTest extends TestCase
{
    use ConfigTrait;
    use RemoveAllTablesTrait;

    protected PDO $connection;

    protected string $dataDir = __DIR__ . '/../data';

    private const COUNT_TABLES = 2000;

    public function setUp(): void
    {
        parent::setUp();
        putenv('KBC_DATADIR=' . $this->dataDir);

        $this->connection = PdoTestConnection::createConnection();

        $this->removeAllTables();

        $sql = [];
        $sql[] = 'CREATE TABLE IF NOT EXISTS t_0000 (';
        $sql[] = 'id INT,';
        // N cols
        for ($i = 0; $i < 20; $i++) {
            $sql[] = sprintf('%04d_c VARCHAR(50) DEFAULT NULL,', $i);
        }

        $sql[] = 'PRIMARY KEY(id)';
        $sql[] = ')';
        $this->connection->query(implode(' ', $sql));

        // M tables
        for ($i = 1; $i < self::COUNT_TABLES; $i++) {
            $this->connection->query(sprintf('CREATE TABLE IF NOT EXISTS t_%04d LIKE t_0000', $i));
        }
    }

    public function testSpeed(): void
    {
        $config = $this->getConfig();

        $config['parameters']['tables'] = [];
        unset($config['parameters']['db']['database']);
        $config['action'] = 'getTables';

        $start = microtime(true);
        JsonHelper::writeFile($this->dataDir . '/config.json', $config);
        $app = new MySQLApplication(new TestLogger());
        ob_start();
        $app->execute();
        /** @var array<array> $result */
        $result = json_decode((string) ob_get_contents(), true);
        ob_end_clean();
        $end = microtime(true);
        $duration = $end-$start;

        echo sprintf('Duration: %.3fs', $duration);
        Assert::assertSame(self::COUNT_TABLES, count($result['tables']));
        Assert::assertLessThan(5.0, $duration); // under 5 seconds
    }
}
