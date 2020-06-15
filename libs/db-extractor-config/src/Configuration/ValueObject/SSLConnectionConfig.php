<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Configuration\ValueObject;

use Keboola\DbExtractorConfig\Exception\PropertyNotSetException;

class SSLConnectionConfig implements ValueObject
{
    private string $key;

    private string $cert;

    private string $ca;

    private string $cipher;

    private bool $verifyServerCert;

    public static function fromArray(array $data): self
    {
        $ssl = $data['db']['ssl'];
        return new self(
            $ssl['key'] ?? null,
            $ssl['cert'] ?? null,
            $ssl['ca'] ?? null,
            $ssl['cipher'] ?? null,
            $ssl['verifyServerCert'] ?? true
        );
    }

    public function __construct(?string $key, ?string $cert, ?string $ca, ?string $cipher, bool $verifyServerCert)
    {
        $this->key = $key;
        $this->cert = $cert;
        $this->ca = $ca;
        $this->cipher = $cipher;
        $this->verifyServerCert = $verifyServerCert;
    }

    public function hasKey()
    {
        return $this->key !== null;
    }

    public function hasCert()
    {
        return $this->cert !== null;
    }

    public function hasCa()
    {
        return $this->ca !== null;
    }

    public function hasCipher()
    {
        return $this->cipher !== null;
    }

    public function getKey(): string
    {
        if ($this->key === null) {
            throw new PropertyNotSetException('Property "key" is not set.');
        }
        return $this->key;
    }

    public function getCert(): string
    {
        if ($this->cert === null) {
            throw new PropertyNotSetException('Property "cert" is not set.');
        }
        return $this->cert;
    }

    public function getCa(): string
    {
        if ($this->ca === null) {
            throw new PropertyNotSetException('Property "ca" is not set.');
        }
        return $this->ca;
    }

    public function getCipher(): string
    {
        if ($this->cipher === null) {
            throw new PropertyNotSetException('Property "cipher" is not set.');
        }
        return $this->cipher;
    }

    public function isVerifyServerCert(): bool
    {
        if ($this->verifyServerCert === null) {
            throw new PropertyNotSetException('Property "verifyServerCert" is not set.');
        }
        return $this->verifyServerCert;
    }
}
