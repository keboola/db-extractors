<?php
/**
 * @package ex-db-mysql
 * @author Erik Zigo <erik.zigo@keboola.com>
 */

use Keboola\DbExtractor\MySQLApplication;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Symfony\Component\Yaml\Yaml;
use Monolog\Handler\NullHandler;
use Monolog\Logger;

define('APP_NAME', 'ex-db-mysql');

require_once(__DIR__ . "/../bootstrap.php");

$logger = new \Keboola\DbExtractor\Logger(getenv('APP_NAME') ? getenv('APP_NAME') : 'ex-db-mysql');

$runAction = true;

try {
    $arguments = getopt("d::", ["data::"]);
    if (!isset($arguments["data"])) {
        throw new UserException('Data folder not set.');
    }

    $config = Yaml::parse(
        file_get_contents($arguments["data"] . "/config.yml")
    );

    $app = new MySQLApplication(
        $config,
        $arguments["data"]
    );

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
    exit(2);
} catch (\Exception $e) {
//	$logger->log('error', $e->getMessage(), [
//		'errFile' => $e->getFile(),
//		'errLine' => $e->getLine(),
//		'trace' => $e->getTrace()
//	]);

    print $e->getMessage();
    print $e->getTraceAsString();

    exit(2);
}
