<?php

declare(strict_types=1);

use Keboola\DbExtractor\Exception\UserException;
use Keboola\Component\Logger;
use Keboola\Component\JsonHelper;
use Keboola\CommonExceptions\UserExceptionInterface;
use Keboola\DbExtractor\OracleApplication;

require __DIR__ . '/../vendor/autoload.php';

$logger = new Logger();
try {
    $app = new OracleApplication($logger);
    $app->execute();
    exit(0);
} catch (UserExceptionInterface $e) {
    $logger->error($e->getMessage());
    exit(1);
} catch (Throwable $e) {
    $logger->critical(
        get_class($e) . ':' . $e->getMessage(),
        [
            'errFile' => $e->getFile(),
            'errLine' => $e->getLine(),
            'errCode' => $e->getCode(),
            'errTrace' => $e->getTraceAsString(),
            'errPrevious' => is_object($e->getPrevious()) ? get_class($e->getPrevious()) : '',
        ]
    );
    exit(2);
}