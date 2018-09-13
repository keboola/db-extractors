<?php

declare(strict_types=1);

use Keboola\DbExtractor\OracleApplication;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Logger;
use Symfony\Component\Yaml\Yaml;
use Monolog\Handler\NullHandler;

require_once(__DIR__ . "/../vendor/autoload.php");

$runAction = true;

try {
    $arguments = getopt("d::", ["data::"]);
    if (!isset($arguments["data"])) {
        throw new UserException('Data folder not set.');
    }

    if (file_exists($arguments["data"] . "/config.yml")) {
        $config = Yaml::parse(
            file_get_contents($arguments["data"] . "/config.yml")
        );
    } else if (file_exists($arguments["data"] . "/config.json")) {
        $config = json_decode(
            file_get_contents($arguments["data"] . "/config.yml"),
            true
        );
    } else {
        throw new UserException('Invalid configuration file type');
    }
    $logger = new Logger('ex-db-oracle');
    $app = new OracleApplication($config, $logger, [], $arguments['data']);

    if ($app['action'] !== 'run') {
        $app['logger']->setHandlers(array(new NullHandler(Logger::INFO)));
        $runAction = false;
    }

    $result = $app->run();

    if (!$runAction) {
        echo json_encode($result);
    }

    $app['logger']->log('info', "Extractor finished successfully.");
    exit(0);
} catch (UserException $e) {
    $logger->log('error', $e->getMessage());
    if (!$runAction) {
        echo $e->getMessage();
    }
    exit(1);
} catch (\Throwable $e) {
    $logger->critical(
        get_class($e) . ':' . $e->getMessage(),
        [
            'errFile' => $e->getFile(),
            'errLine' => $e->getLine(),
            'errCode' => $e->getCode(),
            'errTrace' => $e->getTraceAsString(),
            'errPrevious' => $e->getPrevious() ? get_class($e->getPrevious()) : '',
        ]
    );
    exit(2);
}
