<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\PDO;

use ErrorException;
use Keboola\DbExtractor\Adapter\Connection\BaseDbConnection;
use Keboola\DbExtractor\Adapter\ValueObject\QueryMetadata;
use Keboola\DbExtractor\Adapter\ValueObject\QueryResult;
use PDO;
use PDOException;
use PDOStatement;
use Psr\Log\LoggerInterface;

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
        ?callable $init = null,
        int $connectMaxRetries = self::CONNECT_DEFAULT_MAX_RETRIES,
        array $userInitQueries = []
    ) {
        // Convert errors to PDOExceptions
        $options[PDO::ATTR_ERRMODE] = PDO::ERRMODE_EXCEPTION;

        $this->dsn = $dsn;
        $this->user = $user;
        $this->password = $password;
        $this->options = $options;
        $this->init = $init;
        parent::__construct($logger, $connectMaxRetries, $userInitQueries);
    }

    protected function connect(): void
    {
        $this->logger->info(sprintf('Creating PDO connection to "%s".', $this->dsn));
        $this->pdo = new PDO($this->dsn, $this->user, $this->password, $this->options);
        if ($this->init) {
            ($this->init)($this->pdo);
        }

        $this->runUserInitQueries();
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

    protected function getQueryMetadata(string $query, PDOStatement $stmt): QueryMetadata
    {
        return new PdoQueryMetadata($stmt);
    }

    protected function doQuery(string $query): QueryResult
    {
        /** @var PDOStatement $stmt */
        $stmt = $this->pdo->prepare($query);
        $stmt->execute();
        $queryMetadata = $this->getQueryMetadata($query, $stmt);
        return new PdoQueryResult($query, $queryMetadata, $stmt);
    }

    protected function getExpectedExceptionClasses(): array
    {
        return array_merge(self::BASE_RETRIED_EXCEPTIONS, [
            PDOException::class,
            ErrorException::class, // eg. ErrorException: Warning: Empty row packet body
        ]);
    }
}
