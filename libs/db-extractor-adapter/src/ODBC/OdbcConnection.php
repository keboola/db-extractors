<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\ODBC;

use Throwable;
use Psr\Log\LoggerInterface;
use Keboola\DbExtractor\Adapter\ValueObject\QueryResult;
use Keboola\DbExtractor\Adapter\Connection\BaseDbConnection;
use Keboola\DbExtractor\Adapter\Exception\OdbcException;

class OdbcConnection extends BaseDbConnection
{
    protected string $dsn;

    protected string $user;

    protected string $password;

    /** @var callable|null */
    protected $init;

    /** @var resource */
    protected $connection;

    public function __construct(
        LoggerInterface $logger,
        string $dsn,
        string $user,
        string $password,
        ?callable $init = null
    ) {
        $this->dsn = $dsn;
        $this->user = $user;
        $this->password = $password;
        $this->init = $init;
        parent::__construct($logger);
    }

    protected function connect(): void
    {
        $this->logger->info(sprintf('Creating ODBC connection to "%s".', $this->dsn));
        try {
            /** @var resource|false $connection */
            $connection = @odbc_connect($this->dsn, $this->user, $this->password);
        } catch (Throwable $e) {
            throw new OdbcException($e->getMessage(), $e->getCode(), $e);
        }

        // "odbc_connect" can generate warning, if "set_error_handler" is not set, so we are checking it manually
        if ($connection === false) {
            throw new OdbcException(odbc_errormsg() . ' ' . odbc_error());
        }

        if ($this->init) {
            ($this->init)($connection);
        }

        $this->connection = $connection;
    }

    public function testConnection(): void
    {
        $this->query('SELECT 1', 1);
    }

    /**
     * @return resource
     */
    public function getConnection()
    {
        return $this->connection;
    }

    public function quote(string $str): string
    {
        return "'" . str_replace("'", "''", $str) . "'";
    }

    public function quoteIdentifier(string $str): string
    {
        return '`' . str_replace('`', '``', $str) . '`';
    }

    protected function doQuery(string $query): QueryResult
    {
        try {
            /** @var resource|false $stmt */
            $stmt = @odbc_exec($this->connection, $query);
        } catch (Throwable $e) {
            throw new OdbcException($e->getMessage(), $e->getCode(), $e);
        }

        // "odbc_exec" can generate warning, if "set_error_handler" is not set, so we are checking it manually
        if ($stmt === false) {
            throw new OdbcException(odbc_errormsg($this->connection) . ' ' . odbc_error($this->connection));
        }

        return new OdbcQueryResult($stmt);
    }

    protected function getExpectedExceptionClasses(): array
    {
        return array_merge(self::BASE_RETRIED_EXCEPTIONS, [
            OdbcException::class,
        ]);
    }
}
