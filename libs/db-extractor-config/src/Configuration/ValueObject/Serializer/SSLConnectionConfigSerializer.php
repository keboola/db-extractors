<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Configuration\ValueObject\Serializer;

use Keboola\DbExtractorConfig\Configuration\ValueObject\SSLConnectionConfig;

/**
 * This class can be used if you need to pass parameters to a external tool (eg. java-oracle-exporter).
 */
class SSLConnectionConfigSerializer implements ISSLConnectionConfigSerializer
{
    public static function serialize(SSLConnectionConfig $SSLConnectionConfig): array
    {
        $config = [
            'enabled' => true,
        ];
        if ($SSLConnectionConfig->hasCa()) {
            $config['ca'] = $SSLConnectionConfig->getCa();
        }
        if ($SSLConnectionConfig->hasCert()) {
            $config['cert'] = $SSLConnectionConfig->getCert();
        }
        if ($SSLConnectionConfig->hasKey()) {
            $config['key'] = $SSLConnectionConfig->getKey();
        }
        if ($SSLConnectionConfig->hasCipher()) {
            $config['cipher'] = $SSLConnectionConfig->getCipher();
        }
        return $config;
    }
}
