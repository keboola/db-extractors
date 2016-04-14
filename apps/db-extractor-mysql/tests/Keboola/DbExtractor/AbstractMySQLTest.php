<?php
/**
 * @package ex-db-mysql
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\DbExtractor;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Configuration\MySQLConfigDefinition;
use Keboola\DbExtractor\Test\ExtractorTest;

abstract class AbstractMySQLTest extends ExtractorTest
{
	/**
	 * @var \PDO
	 */
	protected $pdo;

	/**
	 * @param string $driver
	 * @return mixed
	 */
	public function getConfig($driver = 'mysql')
	{
		$config = parent::getConfig($driver);
		$config['extractor_class'] = 'MySQL';
		$config['data_dir'] = $this->dataDir;
		return $config;
	}

	/**
	 * @param CsvFile $file
	 * @return string
	 */
	protected function generateTableName(CsvFile $file)
	{
		$tableName = sprintf(
			'%s',
			$file->getBasename('.' . $file->getExtension())
		);

		return $tableName;
	}

	/**
	 * Create table from csv file with text columns
	 *
	 * @param CsvFile $file
	 */
	protected function createTextTable(CsvFile $file)
	{
		$tableName = $this->generateTableName($file);

		$this->pdo->exec(sprintf(
			'DROP TABLE IF EXISTS %s',
			$tableName
		));

		$this->pdo->exec(sprintf(
			'CREATE TABLE %s (%s) DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;',
			$tableName,
			implode(
				', ',
				array_map(function ($column) {
					return $column . ' text NULL';
				}, $file->getHeader())
			),
			$tableName
		));

		$query = "
			LOAD DATA LOCAL INFILE '{$file}'
			INTO TABLE `{$tableName}`
			CHARACTER SET utf8
			FIELDS TERMINATED BY ','
			OPTIONALLY ENCLOSED BY '\"'
			ESCAPED BY ''
			IGNORE 1 LINES
		";

		$this->pdo->exec($query);

		$count = $this->pdo->query(sprintf('SELECT COUNT(*) AS itemsCount FROM %s', $tableName))->fetchColumn();
		$this->assertEquals($this->countTable($file), (int) $count);
	}

	/**
	 * Count records in CSV (with headers)
	 *
	 * @param CsvFile $file
	 * @return int
	 */
	protected function countTable(CsvFile $file)
	{
		$linesCount = 0;
		foreach ($file AS $i => $line)
		{
			// skip header
			if (!$i) {
				continue;
			}

			$linesCount++;
		}

		return $linesCount;
	}

	/**
	 * @param array $config
	 * @return Application
	 */
	public function createApplication(array $config)
	{
		$app = new Application($config);
		$app->setConfigDefinition(new MySQLConfigDefinition());

		return $app;
	}
}
