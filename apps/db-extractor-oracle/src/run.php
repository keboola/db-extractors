<?php
/**
 * @package ex-db-oracle
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
use Keboola\DbExtractor\OracleApplication;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Symfony\Component\Yaml\Yaml;

define('APP_NAME', 'ex-db-oracle');

require_once(__DIR__ . "/../bootstrap.php");

try {

	$arguments = getopt("d::", ["data::"]);
	if (!isset($arguments["data"])) {
		throw new UserException('Data folder not set.');
	}

	$app = new OracleApplication(
		Yaml::parse(
			file_get_contents($arguments["data"] . "/config.yml")
		),
		$arguments["data"]
	);

	echo json_encode($app->run());
} catch(UserException $e) {

	$app['logger']->log('error', $e->getMessage(), (array) $e->getData());
	exit(1);

} catch(ApplicationException $e) {

	$app['logger']->log('error', $e->getMessage(), (array) $e->getData());
	exit($e->getCode() > 1 ? $e->getCode(): 2);

} catch(\Exception $e) {

	$app['logger']->log('error', $e->getMessage(), [
		'errFile' => $e->getFile(),
		'errLine' => $e->getLine(),
		'trace' => $e->getTrace()
	]);
	exit(2);
}

$app['logger']->log('info', "Extractor finished successfully.");
exit(0);
