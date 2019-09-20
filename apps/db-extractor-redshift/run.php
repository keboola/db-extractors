<?php

use Keboola\DbExtractor\Application;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorConfig\Exception\UserException as ConfigUserException;
use Monolog\Handler\NullHandler;
use Monolog\Logger;
use Symfony\Component\Yaml\Yaml;

require_once(dirname(__FILE__) . "/vendor/autoload.php");

$logger = new \Keboola\DbExtractorLogger\Logger('ex-db-redshift');

try {
    $runAction = true;

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
            file_get_contents($arguments["data"] . '/config.json'),
            true
        );
    } else {
        throw new UserException('Configuration file not found.');
    }
    
    $config['parameters']['data_dir'] = $arguments['data'];
    $config['parameters']['extractor_class'] = 'Redshift';

    $app = new Application($config, $logger);

    if ($app['action'] !== 'run') {
        $app['logger']->setHandlers(array(new NullHandler(Logger::INFO)));
        $runAction = false;
    }
    $result = $app->run();
    if (!$runAction) {
        echo json_encode($result);
    }

} catch(UserException|ConfigUserException $e) {
    $logger->log('error', $e->getMessage(), (array) $e->getData());
    exit(1);
} catch(ApplicationException $e) {
    $logger->log('error', $e->getMessage(), (array) $e->getData());
    exit($e->getCode() > 1 ? $e->getCode(): 2);
} catch(\Exception $e) {
    $logger->log('error', $e->getMessage(), [
        'errFile' => $e->getFile(),
        'errLine' => $e->getLine(),
        'code' => $e->getCode(),
        'trace' => $e->getTrace()
    ]);
    exit(2);
}
exit(0);
