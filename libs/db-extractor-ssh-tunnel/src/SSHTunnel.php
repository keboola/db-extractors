<?php

declare(strict_types=1);

namespace Keboola\DbExtractorSSHTunnel;

use Keboola\DbExtractorSSHTunnel\Exception\UserException;
use Keboola\SSHTunnel\SSH;
use Keboola\SSHTunnel\SSHException;
use Psr\Log\LoggerInterface;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;
use Throwable;

class SSHTunnel
{

    public const DEFAULT_LOCAL_PORT = '33006';

    public const DEFAULT_SSH_PORT = 22;

    public const DEFAULT_MAX_TRIES = 5;

    protected LoggerInterface $logger;

    public function __construct(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }

    public function createSshTunnel(array $dbConfig): array
    {
        // check main param
        foreach (['ssh'] as $k) {
            if (empty($dbConfig[$k])) {
                throw new UserException(sprintf("Main parameter '%s' is missing.", $k));
            }
        }

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

        if (isset($sshConfig['keys']['#private'])) {
            $sshConfig['privateKey'] = $sshConfig['keys']['#private'];
        } else {
            $sshConfig['privateKey'] = $sshConfig['keys']['private'];
            $this->logger->warning('Using unencrypted private key');
        }

        $tunnelParams = array_intersect_key(
            $sshConfig,
            array_flip(
                [
                    'user', 'sshHost', 'sshPort', 'localPort', 'remoteHost', 'remotePort', 'privateKey', 'compression',
                ],
            ),
        );
        $this->logger->info(
            sprintf(
                "Creating SSH tunnel to '%s' on local port '%s'",
                $tunnelParams['sshHost'],
                $tunnelParams['localPort'],
            ),
        );

        $simplyRetryPolicy = new SimpleRetryPolicy(
            $sshConfig['maxRetries'] ?? self::DEFAULT_MAX_TRIES,
            [SSHException::class, Throwable::class],
        );

        $exponentialBackOffPolicy = new ExponentialBackOffPolicy();
        $proxy = new RetryProxy(
            $simplyRetryPolicy,
            $exponentialBackOffPolicy,
            $this->logger,
        );

        try {
            $proxy->call(function () use ($tunnelParams): void {
                $ssh = new SSH();
                $ssh->openTunnel($tunnelParams);
            });
        } catch (SSHException $e) {
            throw new UserException($e->getMessage() . 'Retries count: ' . $proxy->getTryCount(), 0, $e);
        }

        $dbConfig['host'] = '127.0.0.1';
        $dbConfig['port'] = $sshConfig['localPort'];

        return $dbConfig;
    }
}
