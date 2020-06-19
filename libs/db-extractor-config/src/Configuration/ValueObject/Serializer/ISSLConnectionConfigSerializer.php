<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Configuration\ValueObject\Serializer;

use Keboola\DbExtractorConfig\Configuration\ValueObject\SSLConnectionConfig;

interface ISSLConnectionConfigSerializer
{
    public static function serialize(SSLConnectionConfig $SSLConnectionConfig): array;
}
