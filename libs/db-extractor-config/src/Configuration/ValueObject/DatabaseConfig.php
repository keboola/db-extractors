<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Configuration\ValueObject;

use Keboola\DbExtractorConfig\Exception\PropertyNotSetException;

class DatabaseConfig
{

    private ?SSLConnectionConfig $sslConnectionConfig;

    private string $host;

    private ?int $port;

    private string $username;

    private string $password;

    private ?string $database;

    private ?string $schema;

    public static function fromArray(array $data): self
    {
        return new self(
            $data['host'],
            $data['port'] ?? null,
            $data['username'],
            $data['#password'],
            $data['database'] ?? null,
            $data['schema'] ?? null,
            $data['ssl'] ? SSLConnectionConfig::fromArray($data['ssl']) : null
        );
    }

    public function __construct(
        string $host,
        ?int $port,
        string $username,
        string $password,
        ?string $database,
        ?string $schema,
        ?SSLConnectionConfig $sslConnectionConfig
    ) {
        $this->host = $host;
        $this->port = $port;
        $this->username = $username;
        $this->password = $password;
        $this->database = $database;
        $this->schema = $schema;
        $this->sslConnectionConfig = $sslConnectionConfig;
    }

    public function hasPort(): bool
    {
        return $this->port !== null;
    }

    public function hasDatabase(): bool
    {
        return $this->database !== null;
    }

    public function hasSchema(): bool
    {
        return $this->schema !== null;
    }

    public function hasSSLConnection(): bool
    {
        return $this->sslConnectionConfig !== null;
    }

    public function getSslConnectionConfig(): ?SSLConnectionConfig
    {
        if ($this->sslConnectionConfig === null) {
            throw new PropertyNotSetException('SSL config is not set.');
        }
        return $this->sslConnectionConfig;
    }

    public function getHost(): string
    {
        return $this->host;
    }

    public function getPort(): ?int
    {
        if ($this->port === null) {
            throw new PropertyNotSetException('Property "port" is not set.');
        }
        return $this->port;
    }

    public function getUsername(): string
    {
        return $this->username;
    }

    public function getPassword(): string
    {
        return $this->password;
    }

    public function getDatabase(): ?string
    {
        if ($this->database === null) {
            throw new PropertyNotSetException('Property "database" is not set.');
        }
        return $this->database;
    }

    public function getSchema(): ?string
    {
        if ($this->schema === null) {
            throw new PropertyNotSetException('Property "schema" is not set.');
        }
        return $this->schema;
    }
}
