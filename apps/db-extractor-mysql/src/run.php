<?php

declare(strict_types=1);

use Keboola\DbExtractor\MySQLApplication;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorLogger\Logger;
use Symfony\Component\Yaml\Yaml;
use Symfony\Component\Serializer\Encoder\JsonDecode;
use Symfony\Component\Serializer\Encoder\JsonEncoder;
use Monolog\Handler\NullHandler;

require_once(dirname(__FILE__) . '/../vendor/autoload.php');

$logger = new Logger('ex-db-mysql');

$runAction = true;

try {
    $jsonDecode = new JsonDecode(true);

    $arguments = getopt('d::', ['data::']);
    if (!isset($arguments['data']) || !is_string($arguments['data'])) {
        throw new UserException('Data folder not set.');
    }
    $dataFolder = $arguments['data'];

    if (file_exists($dataFolder . '/config.yml')) {
        $config = Yaml::parse(
            (string) file_get_contents($dataFolder . '/config.yml')
        );
    } else if (file_exists($dataFolder . '/config.json')) {
        $config = $jsonDecode->decode(
            (string) file_get_contents($dataFolder . '/config.json'),
            JsonEncoder::FORMAT
        );
    } else {
        throw new UserException('Configuration file not found.');
    }

    // get the state
    $inputState = [];
    $inputStateFile = $arguments['data'] . '/in/state.json';
    if (file_exists($inputStateFile)) {
        $inputState = $jsonDecode->decode(
            (string) file_get_contents($inputStateFile),
            JsonEncoder::FORMAT
        );
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
        echo json_encode($result);
    } else {
        if (!empty($result['state'])) {
            // write state
            $outputStateFile = $arguments['data'] . '/out/state.json';
            $jsonEncode = new \Symfony\Component\Serializer\Encoder\JsonEncode();
            file_put_contents($outputStateFile, $jsonEncode->encode($result['state'], JsonEncoder::FORMAT));
        }
    }
    $app['logger']->log('info', 'Extractor finished successfully.');
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
            'errPrevious' => is_object($e->getPrevious()) ? get_class($e->getPrevious()) : '',
        ]
    );
    exit(2);
}
