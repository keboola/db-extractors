<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;


use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorLogger\Logger;
use Keboola\SSHTunnel\SSH;
use Keboola\SSHTunnel\SSHException;

class SSHTunnel
{

    public const DEFAULT_LOCAL_PORT = 33006;

    public const DEFAULT_SSH_PORT = 22;

    /** @var Logger */
    protected $logger;

    public function __construct(Logger $logger)
    {
        $this->logger = $logger;
    }

    public function createSshTunnel(array $dbConfig): array
    {
        $sshConfig = $dbConfig['ssh'];
        // check params
        foreach (['keys', 'sshHost'] as $k) {
            if (empty($sshConfig[$k])) {
                throw new UserException(sprintf("Parameter '%s' is missing.", $k));
            }
        }

        $sshConfig['remoteHost'] = $dbConfig['host'];
        $sshConfig['remotePort'] = $dbConfig['port'];

        if (empty($sshConfig['user'])) {
            $sshConfig['user'] = $dbConfig['user'];
        }
        if (empty($sshConfig['localPort'])) {
            $sshConfig['localPort'] = self::DEFAULT_LOCAL_PORT;
        }
        if (empty($sshConfig['sshPort'])) {
            $sshConfig['sshPort'] = self::DEFAULT_SSH_PORT;
        }
        $sshConfig['privateKey'] = isset($sshConfig['keys']['#private'])
            ?$sshConfig['keys']['#private']
            :$sshConfig['keys']['private'];
        $tunnelParams = array_intersect_key(
            $sshConfig,
            array_flip(
                [
                    'user', 'sshHost', 'sshPort', 'localPort', 'remoteHost', 'remotePort', 'privateKey', 'compression',
                ]
            )
        );
        $this->logger->info("Creating SSH tunnel to '" . $tunnelParams['sshHost'] . "'");
        $proxy = new RetryProxy(
            $this->logger,
            RetryProxy::DEFAULT_MAX_TRIES,
            RetryProxy::DEFAULT_BACKOFF_INTERVAL,
            ['SSHException', 'Exception']
        );
        try {
            $proxy->call(function () use ($tunnelParams):void {
                $ssh = new SSH();
                $ssh->openTunnel($tunnelParams);
            });
        } catch (SSHException $e) {
            throw new UserException($e->getMessage(), 0, $e);
        }

        $dbConfig['host'] = '127.0.0.1';
        $dbConfig['port'] = $sshConfig['localPort'];

        return $dbConfig;
    }
}