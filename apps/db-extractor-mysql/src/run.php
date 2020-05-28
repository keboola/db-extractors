<?php

declare(strict_types=1);

use Keboola\DbExtractor\MySQLApplication;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorConfig\Exception\UserException as ConfigUserException;
use Keboola\DbExtractorLogger\Logger;
use Keboola\Component\JsonHelper;
use Monolog\Handler\NullHandler;

require_once(dirname(__FILE__) . '/../vendor/autoload.php');

$logger = new Logger('ex-db-mysql');

$runAction = true;

try {
    $arguments = getopt('d::', ['data::']);
    if (!isset($arguments['data']) || !is_string($arguments['data'])) {
        throw new UserException('Data folder not set.');
    }
    $dataFolder = $arguments['data'];

    if (file_exists($dataFolder . '/config.json')) {
        $config = JsonHelper::readFile($dataFolder . '/config.json');
    } else {
        throw new UserException('Configuration file not found.');
    }

    // get the state
    $inputState = [];
    $inputStateFile = $dataFolder . '/in/state.json';
    if (file_exists($inputStateFile)) {
        $inputState = JsonHelper::readFile($inputStateFile);
    }

    $app = new MySQLApplication(
        $config,
        $logger,
        $inputState,
        $dataFolder
    );

    if ($app['action'] !== 'run') {
        $app['logger']->setHandlers(array(new NullHandler(Logger::INFO)));
        $runAction = false;
    }

    $result = $app->run();

    if (!$runAction) {
        echo JsonHelper::encode($result);
    } else {
        if (!empty($result['state'])) {
            // write state
            $outputStateFile = $dataFolder . '/out/state.json';
            JsonHelper::writeFile($outputStateFile, $result['state']);
        }
    }
    $app['logger']->log('info', 'Extractor finished successfully.');
    exit(0);
} catch (UserException|ConfigUserException $e) {
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
            'errPrevious' => is_object($e->getPrevious()) ? get_class($e->getPrevious()) : '',
        ]
    );
    exit(2);
}
