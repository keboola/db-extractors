<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Tests\ValueObject;

use Keboola\DbExtractorConfig\Configuration\ValueObject\SSLConnectionConfig;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

class SSLConnectionConfigTest extends TestCase
{
    public function testValid(): void
    {
        $sslConnectionConfig = SSLConnectionConfig::fromArray([
            'db' => [
                'ssl' => [
                    'key' => 'testKey',
                    'ca' => 'testCa',
                    'cipher' => 'testCipher',
                    'cert' => 'testCertificate',
                    'verifyServerCert' => false,
                ],
            ],
        ]);

        Assert::assertTrue($sslConnectionConfig->hasKey());
        Assert::assertTrue($sslConnectionConfig->hasCa());
        Assert::assertTrue($sslConnectionConfig->hasCert());
        Assert::assertTrue($sslConnectionConfig->hasCipher());
        Assert::assertEquals('testKey', $sslConnectionConfig->getKey());
        Assert::assertEquals('testCa', $sslConnectionConfig->getCa());
        Assert::assertEquals('testCertificate', $sslConnectionConfig->getCert());
        Assert::assertEquals('testCipher', $sslConnectionConfig->getCipher());
        Assert::assertFalse($sslConnectionConfig->isVerifyServerCert());
    }
}
