<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\PDO;

use PDO;
use PDOStatement;
use PDOException;
use ErrorException;
use Psr\Log\LoggerInterface;
use Keboola\DbExtractor\Adapter\ValueObject\QueryResult;
use Keboola\DbExtractor\Adapter\Connection\BaseDbConnection;

class PdoConnection extends BaseDbConnection
{
    protected string $dsn;

    protected string $user;

    protected string $password;

    protected array $options;

    /** @var callable|null */
    protected $init;

    protected PDO $pdo;

    public function __construct(
        LoggerInterface $logger,
        string $dsn,
        string $user,
        string $password,
        array $options,
        ?callable $init = null
    ) {
        // Convert errors to PDOExceptions
        $options[PDO::ATTR_ERRMODE] = PDO::ERRMODE_EXCEPTION;

        $this->dsn = $dsn;
        $this->user = $user;
        $this->password = $password;
        $this->options = $options;
        $this->init = $init;
        parent::__construct($logger);
    }

    protected function connect(): void
    {
        $this->logger->info(sprintf('Creating PDO connection to "%s".', $this->dsn));
        $this->pdo = new PDO($this->dsn, $this->user, $this->password, $this->options);
        if ($this->init) {
            ($this->init)($this->pdo);
        }
    }

    public function testConnection(): void
    {
        $this->query('SELECT 1', 1);
    }

    public function getConnection(): PDO
    {
        return $this->pdo;
    }

    public function quote(string $str): string
    {
        return $this->pdo->quote($str);
    }

    public function quoteIdentifier(string $str): string
    {
        return '`' . str_replace('`', '``', $str) . '`';
    }

    protected function doQuery(string $query): QueryResult
    {
        /** @var PDOStatement $stmt */
        $stmt = $this->pdo->prepare($query);
        $stmt->execute();
        return new PdoQueryResult($stmt);
    }

    protected function getExpectedExceptionClasses(): array
    {
        return array_merge(self::BASE_RETRIED_EXCEPTIONS, [
            PDOException::class,
            ErrorException::class, // eg. ErrorException: Warning: Empty row packet body
        ]);
    }
}
