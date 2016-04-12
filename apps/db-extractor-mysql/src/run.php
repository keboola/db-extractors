<?php
/**
 * @package ex-db-mysql
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
use Keboola\DbExtractor\Application;
use Keboola\DbExtractor\Configuration\MySQLConfigDefinition;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Symfony\Component\Yaml\Yaml;

define('APP_NAME', 'ex-db-mysql');

require_once(__DIR__ . "/../bootstrap.php");

try {

	$arguments = getopt("d::", ["data::"]);
	if (!isset($arguments["data"])) {
		throw new UserException('Data folder not set.');
	}

	$config = Yaml::parse(file_get_contents($arguments["data"] . "/config.yml"));
	$config['data_dir'] = $arguments['data'];
	$config['extractor_class'] = 'MSSQL';

	$app = new Application($config);
	$app->setConfigDefinition(new MySQLConfigDefinition());
	$app->run();

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
