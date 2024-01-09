<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\Tests\Traits;

use Ihsw\Toxiproxy\Proxy;
use Ihsw\Toxiproxy\StreamDirections;
use Ihsw\Toxiproxy\ToxicTypes;
use Ihsw\Toxiproxy\Toxiproxy;
use Psr\Log\Test\TestLogger;

trait ToxiProxyTrait
{
    protected TestLogger $logger;

    protected Toxiproxy $toxiproxy;

    protected function createProxyToDb(): Proxy
    {
        return $this->toxiproxy->create('mariadb_proxy', 'mariadb:3306');
    }

    protected function clearAllProxies(): void
    {
        foreach ($this->toxiproxy->getAll() as $proxy) {
            $this->toxiproxy->delete($proxy);
        }
    }

    protected function makeProxyDown(Proxy $proxy): void
    {
        $proxy->create(ToxicTypes::TIMEOUT, StreamDirections::DOWNSTREAM, 1.0, [
            'timeout' => 1,
        ]);
    }
}
