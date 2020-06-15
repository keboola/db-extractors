<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Configuration\ValueObject;

class SSLConnectionConfig implements ValueObject
{
    private ?string $key;

    private ?string $cert;

    private ?string $ca;

    private ?string $cipher;

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

    public function getKey(): ?string
    {
        return $this->key;
    }

    public function getCert(): ?string
    {
        return $this->cert;
    }

    public function getCa(): ?string
    {
        return $this->ca;
    }

    public function getCipher(): ?string
    {
        return $this->cipher;
    }

    public function isVerifyServerCert(): bool
    {
        return $this->verifyServerCert;
    }
}
