<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Tests\ValueObject;

use Keboola\CommonExceptions\UserExceptionInterface;
use Keboola\DbExtractorConfig\Configuration\ValueObject\Serializer\SSLConnectionConfigSerializer;
use Keboola\DbExtractorConfig\Configuration\ValueObject\SSLConnectionConfig;
use Keboola\DbExtractorConfig\Exception\PropertyNotSetException;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

class SSLConnectionConfigTest extends TestCase
{
    public function testValid(): void
    {
        $sslConnectionConfig = SSLConnectionConfig::fromArray([
            '#key' => 'testKey',
            'ca' => 'testCa',
            'cipher' => 'testCipher',
            'cert' => 'testCertificate',
            'verifyServerCert' => false,
            'ignoreCertificateCn' => true,
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
        Assert::assertTrue($sslConnectionConfig->isIgnoreCertificateCn());
    }

    public function testMissingConfigProperty(): void
    {
        $sslConnectionConfig = SSLConnectionConfig::fromArray([]);

        Assert::assertFalse($sslConnectionConfig->hasKey());
        try {
            $sslConnectionConfig->getKey();
            Assert::fail('Property "key" is exists.');
        } catch (PropertyNotSetException $e) {
            Assert::assertEquals('Property "key" is not set.', $e->getMessage());
        }

        Assert::assertFalse($sslConnectionConfig->hasCa());
        try {
            $sslConnectionConfig->getCa();
            Assert::fail('Property "ca" is exists.');
        } catch (PropertyNotSetException $e) {
            Assert::assertEquals('Property "ca" is not set.', $e->getMessage());
        }

        Assert::assertFalse($sslConnectionConfig->hasCert());
        try {
            $sslConnectionConfig->getCert();
            Assert::fail('Property "cert" is exists.');
        } catch (PropertyNotSetException $e) {
            Assert::assertEquals('Property "cert" is not set.', $e->getMessage());
        }

        Assert::assertFalse($sslConnectionConfig->hasCipher());
        try {
            $sslConnectionConfig->getCipher();
            Assert::fail('Property "cipher" is exists.');
        } catch (PropertyNotSetException $e) {
            Assert::assertEquals('Property "cipher" is not set.', $e->getMessage());
        }
    }

    public function testSSLConnectionConfigSerializer(): void
    {
        $input = [
            'enabled' => true,
            '#key' => 'testKey',
            'ca' => 'testCa',
            'cert' => 'testCert',
        ];

        $expected = [
            'enabled' => true,
            'ca' => 'testCa',
            'cert' => 'testCert',
            'key' => 'testKey',
        ];

        $serialize = SSLConnectionConfigSerializer::serialize(SSLConnectionConfig::fromArray($input));
        Assert::assertEquals($expected, $serialize);
    }
}
