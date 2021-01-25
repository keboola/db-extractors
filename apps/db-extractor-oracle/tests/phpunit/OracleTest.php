<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Component\Logger;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Extractor\Oracle;
use Keboola\DbExtractor\Tests\Traits\ConfigTrait;
use Keboola\DbExtractor\TraitTests\CloseSshTunnelsTrait;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use PHPUnit\Framework\TestCase;

class OracleTest extends TestCase
{
    use ConfigTrait;
    use CloseSshTunnelsTrait;

    public function testSSHTestConnectionFailed(): void
    {
        $config = $this->getConfigConnection();

        // Create extractor: SSH tunnel is created
        $extractor = new Oracle($config['parameters'], [], new Logger());

        // Kill SSH tunnel
        $this->closeSshTunnels();

        // Test connection must fail.
        // Test whether the SSH tunnel is really used,
        // because the direct connection is also available in the test environment.
        $this->expectException(UserException::class);
        $this->expectExceptionMessage('The Network Adapter could not establish the connection');
        $extractor->testConnection();
    }

    public function testSSHRunConnectionFailed(): void
    {
        $config = $this->getConfig();

        // Create extractor: SSH tunnel is created
        $extractor = new Oracle($config['parameters'], [], new Logger());

        // Kill SSH tunnel
        $this->closeSshTunnels();

        // Export must fail
        // Test whether the SSH tunnel is really used,
        // because the direct connection is also available in the test environment.
        $this->expectException(UserException::class);
        $this->expectExceptionMessage('The Network Adapter could not establish the connection');
        $extractor->export(ExportConfig::fromArray($config['parameters']));
    }
}
