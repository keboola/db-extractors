<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\Tests\ODBC;

use Keboola\CommonExceptions\UserExceptionInterface;
use Keboola\DbExtractor\Adapter\Connection\DbConnection;
use Keboola\DbExtractor\Adapter\Exception\DeadConnectionException;
use Keboola\DbExtractor\Adapter\Tests\BaseTest;
use Keboola\DbExtractor\Adapter\Tests\Traits\OdbcCreateConnectionTrait;
use Keboola\DbExtractor\Adapter\ValueObject\QueryResult;
use PHPUnit\Framework\Assert;

class OdbcConnectionTest extends BaseTest
{
    use OdbcCreateConnectionTrait;

    public function testInvalidHost(): void
    {
        $retries = 2;
        try {
            $this->createOdbcConnection('invalid');
            Assert::fail('Exception expected.');
        } catch (UserExceptionInterface $e) {
            Assert::assertStringContainsString('Error connecting to DB: ', $e->getMessage());
            Assert::assertStringContainsString('Unknown MySQL server host \'invalid\'', $e->getMessage());
        }

        for ($attempt=1; $attempt < $retries; $attempt++) {
            Assert::assertTrue($this->logger->hasInfoThatContains("Retrying... [{$attempt}x]"));
        }
    }

    public function testInvalidHostNoErrorHandler(): void
    {
        // Disable error handler, so "odbc_connect" generates warning and not exception, returns false;
        set_error_handler(null);
        $retries = 2;
        try {
            $this->createOdbcConnection('invalid', null, $retries);
            Assert::fail('Exception expected.');
        } catch (UserExceptionInterface $e) {
            Assert::assertStringContainsString('Error connecting to DB: ', $e->getMessage());
            Assert::assertStringContainsString('Unknown MySQL server host \'invalid\'', $e->getMessage());
        }

        for ($attempt=1; $attempt < $retries; $attempt++) {
            Assert::assertTrue($this->logger->hasInfoThatContains("Retrying... [{$attempt}x]"));
        }
    }

    public function testDisableConnectRetries(): void
    {
        // Disable error handler, so "odbc_connect" generates warning and not exception, returns false;
        set_error_handler(null);
        try {
            $this->createOdbcConnection('invalid', null, 1);
            Assert::fail('Exception expected.');
        } catch (UserExceptionInterface $e) {
            Assert::assertStringContainsString('Error connecting to DB: ', $e->getMessage());
            Assert::assertStringContainsString('Unknown MySQL server host \'invalid\'', $e->getMessage());
        }

        // No retry in logs
        Assert::assertFalse($this->logger->hasInfoThatContains('Retrying... '));
    }

    public function testTestConnection(): void
    {
        $connection = $this->createOdbcConnection();
        $connection->testConnection();
        Assert::assertTrue($this->logger->hasInfoThatContains(
            'Creating ODBC connection to "Driver={MariaDB ODBC Driver};SERVER=mariadb;PORT=3306;DATABASE=testdb;".',
        ));
    }

    public function testTestConnectionFailed(): void
    {
        $proxy = $this->createProxyToDb();
        $connection = $this->createOdbcConnection(self::TOXIPROXY_HOST, (int) $proxy->getListenPort());
        $this->makeProxyDown($proxy);

        try {
            $connection->testConnection();
            Assert::fail('Exception expected.');
        } catch (UserExceptionInterface $e) {
            Assert::assertStringContainsString('Lost connection to MySQL server', $e->getMessage());
        }
    }

    public function testTestConnectionFailedNoErrorHandler(): void
    {
        // Disable error handler, so "odbc_exec" generates warning and not exception, returns false;
        set_error_handler(null);
        $proxy = $this->createProxyToDb();
        $connection = $this->createOdbcConnection(self::TOXIPROXY_HOST, (int) $proxy->getListenPort());
        $this->makeProxyDown($proxy);

        try {
            $connection->testConnection();
            Assert::fail('Exception expected.');
        } catch (UserExceptionInterface $e) {
            Assert::assertStringContainsString('Lost connection to MySQL server', $e->getMessage());
        }
    }

    public function testGetConnection(): void
    {
        $connection = $this->createOdbcConnection();
        Assert::assertTrue(is_resource($connection->getConnection()));
    }

    public function testQuote(): void
    {
        $connection = $this->createOdbcConnection();
        Assert::assertSame("'abc'''", $connection->quote("abc'"));
    }

    public function testQuoteIdentifier(): void
    {
        $connection = $this->createOdbcConnection();
        Assert::assertSame('`abc```', $connection->quoteIdentifier('abc`'));
    }

    public function testIsAlive(): void
    {
        $connection = $this->createOdbcConnection();
        $connection->isAlive();
        $this->expectNotToPerformAssertions();
    }

    public function testIsAliveFailed(): void
    {
        $proxy = $this->createProxyToDb();
        $connection = $this->createOdbcConnection(self::TOXIPROXY_HOST, (int) $proxy->getListenPort());
        $this->makeProxyDown($proxy);

        try {
            $connection->isAlive();
            Assert::fail('Exception expected.');
        } catch (DeadConnectionException $e) {
            Assert::assertStringContainsString('Dead connection:', $e->getMessage());
            Assert::assertStringContainsString('Lost connection to MySQL server', $e->getMessage());
        }
    }

    public function testQuery(): void
    {
        $connection = $this->createOdbcConnection();
        Assert::assertSame(
            [['X' => '123', 'Y' => '456']],
            $connection->query('SELECT 123 as X, 456 as Y')->fetchAll(),
        );
        Assert::assertTrue($this->logger->hasDebug('Running query "SELECT 123 as X, 456 as Y".'));
    }

    public function testQueryFailed(): void
    {
        $proxy = $this->createProxyToDb();
        $connection = $this->createOdbcConnection(self::TOXIPROXY_HOST, (int) $proxy->getListenPort());
        $this->makeProxyDown($proxy);

        $retries = 4;
        try {
            $connection->query('SELECT 123 as X, 456 as Y', $retries);
            Assert::fail('Exception expected.');
        } catch (UserExceptionInterface $e) {
            Assert::assertStringContainsString('Lost connection to MySQL server', $e->getMessage());
        }

        for ($attempt=1; $attempt < $retries; $attempt++) {
            Assert::assertTrue($this->logger->hasInfoThatContains("Retrying... [{$attempt}x]"));
        }
    }

    public function testQueryAndProcess(): void
    {
        $connection = $this->createOdbcConnection();
        Assert::assertSame(
            [['X', 'Y' ]],
            $connection->queryAndProcess('SELECT 123 as X, 456 as Y', 3, function (QueryResult $result) {
                return array_map(function (array $row) {
                    return array_keys($row);
                }, $result->fetchAll());
            }),
        );
    }

    public function testQueryAndProcessFailed(): void
    {
        $proxy = $this->createProxyToDb();
        $connection = $this->createOdbcConnection(self::TOXIPROXY_HOST, (int) $proxy->getListenPort());
        $this->makeProxyDown($proxy);

        $retries = 4;
        try {
            $connection->queryAndProcess('SELECT 123 as X, 456 as Y', $retries, function (QueryResult $result) {
                return array_map(function (array $row) {
                    return array_keys($row);
                }, $result->fetchAll());
            });
            Assert::fail('Exception expected.');
        } catch (UserExceptionInterface $e) {
            Assert::assertStringContainsString('Lost connection to MySQL server', $e->getMessage());
        }

        for ($attempt=1; $attempt < $retries; $attempt++) {
            Assert::assertTrue($this->logger->hasInfoThatContains("Retrying... [{$attempt}x]"));
        }
    }
}
