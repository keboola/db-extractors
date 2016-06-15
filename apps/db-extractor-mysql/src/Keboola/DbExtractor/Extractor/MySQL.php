<?php
/**
 * @package ex-db-mysql
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Exception\UserException;
use Keboola\Temp\Temp;

class MySQL extends Extractor
{
	/**
	 * @param $sslCa
	 * @param Temp $temp
	 * @return string
	 */
	private function createSSLFile($sslCa, Temp $temp)
	{
		$filename = $temp->createTmpFile('ssl');
		file_put_contents($filename, $sslCa);
		return realpath($filename);
	}

	public function createConnection($params)
	{
		$isSsl = false;

		// convert errors to PDOExceptions
		$options = [
			\PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
		];

		// ssl encryption
		if (!empty($params['ssl']) && !empty($params['ssl']['enabled'])) {
			$ssl = $params['ssl'];

			$temp = new Temp(defined('APP_NAME') ? APP_NAME : 'ex-db-mysql');

			if (!empty($ssl['key'])) {
				$options[\PDO::MYSQL_ATTR_SSL_KEY] = $this->createSSLFile($ssl['key'], $temp);
				$isSsl = true;
			}
			if (!empty($ssl['cert'])) {
				$options[\PDO::MYSQL_ATTR_SSL_CERT] = $this->createSSLFile($ssl['cert'], $temp);
				$isSsl = true;
			}
			if (!empty($ssl['ca'])) {
				$options[\PDO::MYSQL_ATTR_SSL_CA] = $this->createSSLFile($ssl['ca'], $temp);
				$isSsl = true;
			}
			if (!empty($ssl['cipher'])) {
				$options[\PDO::MYSQL_ATTR_SSL_CIPHER] = $ssl['cipher'];
			}
		}

		foreach (['host', 'database', 'user', 'password'] as $r) {
			if (!array_key_exists($r, $params)) {
				throw new UserException(sprintf("Parameter %s is missing.", $r));
			}
		}

		$port = !empty($params['port']) ? $params['port'] : '3306';
		$dsn = sprintf(
			"mysql:host=%s;port=%s;dbname=%s;charset=utf8",
			$params['host'],
			$port,
			$params['database']
		);

		$this->logger->info("Connecting to DSN '" . $dsn . "' " . ($isSsl ? 'Using SSL' : ''));

		$pdo = new \PDO($dsn, $params['user'], $params['password'], $options);
		$pdo->setAttribute(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
		$pdo->exec("SET NAMES utf8;");

		if ($isSsl) {
			$status = $pdo->query("SHOW STATUS LIKE 'Ssl_cipher';")->fetch(\PDO::FETCH_ASSOC);

			if (empty($status['Value'])) {
				throw new UserException(sprintf("Connection is not encrypted"));
			} else {
				$this->logger->info("Using SSL cipher: " . $status['Value']);
			}
		}

		return $pdo;
	}

	public function getConnection()
	{
		return $this->db;
	}

	public function testConnection()
	{
		$this->db->query('SELECT NOW();')->execute();
	}
}
