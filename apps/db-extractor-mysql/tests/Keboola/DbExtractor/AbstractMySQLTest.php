<?php
/**
 * @package ex-db-mysql
 * @author Erik Zigo <erik.zigo@keboola.com>
 */

namespace Keboola\DbExtractor\Tests;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\MySQLApplication;
use Keboola\DbExtractor\Test\ExtractorTest;

abstract class AbstractMySQLTest extends ExtractorTest
{
    const DRIVER = 'mysql';

    /** @var \PDO */
    protected $pdo;

    protected $appName;

    protected $rootPath;

    public function setUp()
    {
        if (!$this->appName) {
            $this->appName = getenv('APP_NAME') ? getenv('APP_NAME') : 'ex-db-mysql';
            if (!defined('APP_NAME')) {
                define('APP_NAME', $this->appName);
            }
        }

        if (!$this->rootPath) {
            $this->rootPath = getenv('ROOT_PATH') ? getenv('ROOT_PATH') : '/code';
        }

        $options = [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
            \PDO::MYSQL_ATTR_LOCAL_INFILE => true
        ];

        $options[\PDO::MYSQL_ATTR_SSL_KEY] = realpath($this->dataDir . '/mysql/ssl/client-key.pem');
        $options[\PDO::MYSQL_ATTR_SSL_CERT] = realpath($this->dataDir . '/mysql/ssl/client-cert.pem');
        $options[\PDO::MYSQL_ATTR_SSL_CA] = realpath($this->dataDir . '/mysql/ssl/ca.pem');

        $config = $this->getConfig(self::DRIVER);
        $dbConfig = $config['parameters']['db'];

        $dsn = sprintf(
            "mysql:host=%s;port=%s;dbname=%s;charset=utf8",
            $dbConfig['host'],
            $dbConfig['port'],
            $dbConfig['database']
        );

        $this->pdo = new \PDO($dsn, $dbConfig['user'], $dbConfig['password'], $options);

        $this->pdo->setAttribute(\PDO::MYSQL_ATTR_LOCAL_INFILE, true);
        $this->pdo->exec("SET NAMES utf8;");
    }

    protected function createAutoIncrementTable()
    {
        $this->pdo->exec('DROP TABLE IF EXISTS `test`.`auto-increment`');

        $this->pdo->exec(
          'CREATE TABLE `test`.`auto-increment` (
            `id` INT NOT NULL AUTO_INCREMENT,
            `name` VARCHAR(30) NOT NULL DEFAULT \'pam\',
            PRIMARY KEY (`id`)  
          )'
        );

        $this->pdo->exec('INSERT INTO `test`.`auto-increment` VALUES (\'george\', \'henry\')');
    }

    protected function createTimestampTable()
    {
        $this->pdo->exec('DROP TABLE IF EXISTS `test`.`timestamp`');

        $this->pdo->exec(
            'CREATE TABLE `test`.`timestamp` (
            `name` VARCHAR(30) NOT NULL DEFAULT \'pam\',
            `timestamp` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
          )'
        );

        $this->pdo->exec('INSERT INTO `test`.`timestamp` VALUES (\'richard\', \'john\')');
    }

    /**
     * @param string $driver
     * @param string $format (yaml || json)
     * @return mixed
     */
    public function getConfig($driver = self::DRIVER, $format = 'yaml')
    {
        $config = parent::getConfig($driver, $format);
        if (!empty($config['parameters']['db']['#password'])) {
            $config['parameters']['db']['password'] = $config['parameters']['db']['#password'];
        }
        $config['parameters']['extractor_class'] = 'MySQL';
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
    protected function createTextTable(CsvFile $file, $tableName = null, $schemaName = null)
    {
        if (!$tableName) {
            $tableName = $this->generateTableName($file);
        }

        if (!$schemaName) {
            $schemaName = "test";
        } else {
            $this->pdo->exec(sprintf("CREATE DATABASE IF NOT EXISTS %s", $schemaName));
        }

        $this->pdo->exec(sprintf(
            'DROP TABLE IF EXISTS %s.%s',
            $schemaName,
            $tableName
        ));

        $this->pdo->exec(sprintf(
            'CREATE TABLE %s.%s (%s) DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;',
            $schemaName,
            $tableName,
            implode(
                ', ',
                array_map(function ($column) {
                    return $column . ' text NULL';
                }, $file->getHeader())
            )
        ));

        $query = "
			LOAD DATA LOCAL INFILE '{$file}'
			INTO TABLE `{$schemaName}`.`{$tableName}`
			CHARACTER SET utf8
			FIELDS TERMINATED BY ','
			OPTIONALLY ENCLOSED BY '\"'
			ESCAPED BY ''
			IGNORE 1 LINES
		";

        $this->pdo->exec($query);

        $count = $this->pdo->query(sprintf('SELECT COUNT(*) AS itemsCount FROM %s.%s', $schemaName, $tableName))->fetchColumn();
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
        foreach ($file as $i => $line) {
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
     * @return MySQLApplication
     */
    public function createApplication(array $config)
    {
        $app = new MySQLApplication($config, $this->dataDir);

        return $app;
    }

    public function configTypesProvider()
    {
        return [
            ['yaml'],
            ['json']
        ];
    }
}
