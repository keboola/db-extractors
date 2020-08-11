<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\Tests\ODBC;

use PHPUnit\Framework\Assert;
use Keboola\DbExtractor\Adapter\Tests\Traits\OdbcCreateConnectionTrait;
use Keboola\DbExtractor\Adapter\Tests\BaseTest;
use Keboola\DbExtractor\Adapter\Exception\DeadConnectionException;
use Keboola\DbExtractor\Adapter\ValueObject\QueryResult;
use Keboola\CommonExceptions\UserExceptionInterface;
use Keboola\DbExtractor\Adapter\Connection\DbConnection;

class OdbcConnectionTest extends BaseTest
{
    use OdbcCreateConnectionTrait;

    public function testInvalidHost(): void
    {
        try {
            $this->createOdbcConnection('invalid');
            Assert::fail('Exception expected.');
        } catch (UserExceptionInterface $e) {
            Assert::assertStringContainsString('Error connecting to DB: odbc_connect():', $e->getMessage());
            Assert::assertStringContainsString('Unknown MySQL server host \'invalid\'', $e->getMessage());
        }

        for ($attempt=1; $attempt < DbConnection::CONNECT_MAX_RETRIES; $attempt++) {
            Assert::assertTrue($this->logger->hasInfoThatContains("Retrying... [{$attempt}x]"));
        }
    }

    public function testTestConnection(): void
    {
        $connection = $this->createOdbcConnection();
        $connection->testConnection();
        Assert::assertTrue($this->logger->hasInfoThatContains(
            'Creating ODBC connection to "Driver={MariaDB ODBC Driver};SERVER=mariadb;PORT=3306;DATABASE=testdb;".'
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
            Assert::assertStringContainsString('Lost connection to MySQL server during query', $e->getMessage());
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
            Assert::assertStringContainsString('Lost connection to MySQL server during query', $e->getMessage());
        }
    }

    public function testQuery(): void
    {
        $connection = $this->createOdbcConnection();
        Assert::assertSame(
            [['X' => '123', 'Y' => '456']],
            $connection->query('SELECT 123 as X, 456 as Y')->fetchAll()
        );
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
            Assert::assertStringContainsString('Connection not open', $e->getMessage());
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
            })
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
            Assert::assertStringContainsString('Connection not open', $e->getMessage());
        }

        for ($attempt=1; $attempt < $retries; $attempt++) {
            Assert::assertTrue($this->logger->hasInfoThatContains("Retrying... [{$attempt}x]"));
        }
    }
}
