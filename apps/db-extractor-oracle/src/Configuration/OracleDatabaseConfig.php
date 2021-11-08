<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

use Keboola\DbExtractor\Configuration\NodeDefinition\OracleDbNode;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\SSLConnectionConfig;
use Keboola\DbExtractorConfig\Exception\PropertyNotSetException;

class OracleDatabaseConfig extends DatabaseConfig
{
    private ?string $host;

    private ?string $tnsnames;

    private int $defaultRowPrefetch;

    private bool $connectThrough;

    public static function fromArray(array $data): self
    {
        $sslEnabled = !empty($data['ssl']) && !empty($data['ssl']['enabled']);

        return new self(
            $data['host'] ?? null,
            !empty($data['port']) ? (string) $data['port'] : null,
            $data['user'],
            $data['#password'],
            $data['database'] ?? null,
            $data['schema'] ?? null,
            $sslEnabled ? SSLConnectionConfig::fromArray($data['ssl']) : null,
            $data['tnsnames'] ?? null,
            $data['connectThrough'] ?? false,
            (int) ($data['defaultRowPrefetch'] ?? OracleDbNode::DEFAULT_ROWS_PREFETCH),
            $data['initQueries'] ?? []
        );
    }

    public function __construct(
        ?string $host,
        ?string $port,
        string $username,
        string $password,
        ?string $database,
        ?string $schema,
        ?SSLConnectionConfig $sslConnectionConfig,
        ?string $tnsnames,
        bool $connectThrough,
        int $defaultRowPrefetch,
        array $userQueries
    ) {
        parent::__construct(
            '',
            $port,
            $username,
            $password,
            $database,
            $schema,
            $sslConnectionConfig,
            $userQueries
        );

        $this->host = $host;
        $this->tnsnames = $tnsnames;
        $this->connectThrough = $connectThrough;
        $this->defaultRowPrefetch = $defaultRowPrefetch;
    }

    public function hasHost(): bool
    {
        return $this->host !== null;
    }

    public function getHost(): string
    {
        if ($this->host === null) {
            throw new PropertyNotSetException('Property "host" is not set.');
        }
        return $this->host;
    }

    public function hasTnsnames(): bool
    {
        return $this->tnsnames !== null;
    }

    public function getTnsnames(): string
    {
        if ($this->tnsnames === null) {
            throw new PropertyNotSetException('Property "tnsnames" is not set.');
        }
        return $this->tnsnames;
    }

    public function isConnectThroughEnabled(): bool
    {
        return $this->connectThrough;
    }

    public function getDefaultRowPrefetch(): int
    {
        return $this->defaultRowPrefetch;
    }

    public function getInitQueries(): array
    {
        return array_map(fn($item) => rtrim($item, ';'), parent::getInitQueries());
    }
}
