<?php

declare(strict_types=1);

use Keboola\DbExtractor\OracleApplication;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Symfony\Component\Yaml\Yaml;
use Monolog\Handler\NullHandler;
use Monolog\Logger;

define('APP_NAME', 'ex-db-oracle');

require_once(__DIR__ . "/../bootstrap.php");

$logger = new \Keboola\DbExtractor\Logger('ex-db-oracle');

$runAction = true;

try {
    $arguments = getopt("d::", ["data::"]);
    if (!isset($arguments["data"])) {
        throw new UserException('Data folder not set.');
    }

    if (file_exists($arguments["data"] . "/config.yml")) {
        $app = new OracleApplication(
            Yaml::parse(
                file_get_contents($arguments["data"] . "/config.yml")
            ),
            $arguments["data"]
        );
    } else if (file_exists($arguments["data"] . "/config.json")) {
        $app = new OracleApplication(
            json_decode(
                file_get_contents($arguments["data"] . "/config.yml"),
                true
            ),
            $arguments["data"]
        );
    } else {
        throw new UserException('Invalid configuration file type');
    }


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
    $logger->log('error', $e->getMessage(), $e->getData());

    if (!$runAction) {
        echo $e->getMessage();
    }

    exit(1);
} catch (ApplicationException $e) {
    $logger->log('error', $e->getMessage(), $e->getData());
    exit($e->getCode() > 1 ? $e->getCode(): 2);
} catch (\Exception $e) {
    $logger->log('error', $e->getMessage(), [
        'errFile' => $e->getFile(),
        'errLine' => $e->getLine(),
        'trace' => $e->getTrace()
    ]);
    exit(2);
}
