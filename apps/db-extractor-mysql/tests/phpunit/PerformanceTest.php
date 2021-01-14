<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Component\Logger;
use Keboola\DbExtractor\FunctionalTests\PdoTestConnection;
use Keboola\DbExtractor\MySQLApplication;
use Keboola\DbExtractor\Tests\Traits\ConfigTrait;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

class PerformanceTest extends TestCase
{
    use ConfigTrait;

    protected string $dataDir = __DIR__ . '/../data';

    public function setUp(): void
    {
        parent::setUp();

        $connection = PdoTestConnection::createConnection();

        $sql = [];
        $sql[] = 'CREATE TABLE IF NOT EXISTS t_0000 (';
        $sql[] = 'id INT,';
        // N cols
        for ($i = 0; $i < 20; $i++) {
            $sql[] = sprintf('%04d_c VARCHAR(50) DEFAULT NULL,', $i);
        }

        $sql[] = 'PRIMARY KEY(id)';
        $sql[] = ')';
        $connection->query(implode(' ', $sql));

        // M tables
        for ($i = 1; $i < 2000; $i++) {
            $connection->query(sprintf('CREATE TABLE IF NOT EXISTS t_%04d LIKE t_0000', $i));
        }
    }

    public function testSpeed(): void
    {
        $config = $this->getConfig();

        $config['parameters']['tables'] = [];
        unset($config['parameters']['db']['database']);
        $config['action'] = 'getTables';

        $start = microtime(true);
        $app = new MySQLApplication($config, new Logger(), [], $this->dataDir);
        $result = $app->run();
        $end = microtime(true);
        $duration = $end-$start;

        echo sprintf('Duration: %.3fs', $duration);
        Assert::assertSame(2000, count($result['tables']));
        Assert::assertLessThan(5.0, $duration); // under 5 seconds
    }
}
