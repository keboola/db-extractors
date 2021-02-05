<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\SSLConnectionConfig;
use Keboola\DbExtractorConfig\Exception\PropertyNotSetException;

class OracleDatabaseConfig extends DatabaseConfig
{
    private ?string $host;

    private ?string $tnsnames;

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
            $data['connectThrough'] ?? false
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
        bool $connectThrough
    ) {
        parent::__construct(
            '',
            $port,
            $username,
            $password,
            $database,
            $schema,
            $sslConnectionConfig
        );

        $this->host = $host;
        $this->tnsnames = $tnsnames;
        $this->connectThrough = $connectThrough;
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
}
