<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Configuration\ValueObject;

use Keboola\DbExtractorConfig\Exception\PropertyNotSetException;

class SSLConnectionConfig implements ValueObject
{
    private ?string $key;

    private ?string $cert;

    private ?string $ca;

    private ?string $cipher;

    private bool $verifyServerCert;

    private bool $ignoreCertificateCn;

    public static function fromArray(array $data): self
    {
        return new self(
            $data['#key'] ?? null,
            $data['cert'] ?? null,
            $data['ca'] ?? null,
            $data['cipher'] ?? null,
            $data['verifyServerCert'] ?? true,
            $data['ignoreCertificateCn'] ?? false,
        );
    }

    public function __construct(
        ?string $key,
        ?string $cert,
        ?string $ca,
        ?string $cipher,
        bool $verifyServerCert,
        bool $ignoreCertificateCn,
    ) {
        $this->key = $key ?: null;
        $this->cert = $cert  ?: null;
        $this->ca = $ca  ?: null;
        $this->cipher = $cipher  ?: null;
        $this->verifyServerCert = $verifyServerCert;
        $this->ignoreCertificateCn = $ignoreCertificateCn;
    }

    public function hasKey(): bool
    {
        return $this->key !== null;
    }

    public function hasCert(): bool
    {
        return $this->cert !== null;
    }

    public function hasCa(): bool
    {
        return $this->ca !== null;
    }

    public function hasCipher(): bool
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
        return $this->verifyServerCert;
    }

    public function isIgnoreCertificateCn(): bool
    {
        return $this->ignoreCertificateCn;
    }
}
