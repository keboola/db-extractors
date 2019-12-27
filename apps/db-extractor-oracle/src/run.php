<?php

declare(strict_types=1);

use Keboola\DbExtractor\OracleApplication;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorConfig\Exception\UserException as ConfigUserException;
use Keboola\DbExtractorLogger\Logger;
use Monolog\Handler\NullHandler;
use Symfony\Component\Serializer\Encoder\JsonDecode;
use Symfony\Component\Serializer\Encoder\JsonEncode;
use Symfony\Component\Serializer\Encoder\JsonEncoder;

require_once(__DIR__ . '/../vendor/autoload.php');

$logger = new Logger('ex-db-oracle');

$runAction = true;

try {
    $arguments = getopt('d::', ['data::']);
    if (!isset($arguments['data']) || !is_string($arguments['data'])) {
        throw new UserException('Data folder not set.');
    }
    $dataFolder = (string) $arguments['data'];

    if (file_exists($dataFolder . '/config.json')) {
        $config = json_decode(
            (string) file_get_contents($dataFolder . '/config.json'),
            true
        );
    } else {
        throw new UserException('Invalid configuration file type');
    }

    // get the state
    $inputState = [];
    $inputStateFile = $arguments['data'] . '/in/state.json';
    if (file_exists($inputStateFile)) {
        $jsonDecode = new JsonDecode(true);
        $inputState = $jsonDecode->decode(
            file_get_contents($inputStateFile),
            JsonEncoder::FORMAT
        );
    }

    $app = new OracleApplication($config, $logger, $inputState, $dataFolder);

    if ($app['action'] !== 'run') {
        $app['logger']->setHandlers(array(new NullHandler(Logger::INFO)));
        $runAction = false;
    }

    $result = $app->run();

    if (!$runAction) {
        echo json_encode($result);
    } else {
        if (!empty($result['state'])) {
            // write state
            $outputStateFile = $arguments['data'] . '/out/state.json';
            $jsonEncode = new JsonEncode();
            file_put_contents($outputStateFile, $jsonEncode->encode($result['state'], JsonEncoder::FORMAT));
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
