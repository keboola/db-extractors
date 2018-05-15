<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Logger;
use Keboola\DbExtractor\MySQLApplication;
use Keboola\DbExtractor\Test\ExtractorTest;
use PDO;

abstract class AbstractMySQLTest extends ExtractorTest
{
    public const DRIVER = 'mysql';

    /** @var PDO */
    protected $pdo;

    public function setUp(): void
    {
        $this->dataDir = __DIR__ . '/../../data';

        $options = [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::MYSQL_ATTR_LOCAL_INFILE => true,
        ];

        $options[PDO::MYSQL_ATTR_SSL_KEY] = realpath($this->dataDir . '/mysql/ssl/client-key.pem');
        $options[PDO::MYSQL_ATTR_SSL_CERT] = realpath($this->dataDir . '/mysql/ssl/client-cert.pem');
        $options[PDO::MYSQL_ATTR_SSL_CA] = realpath($this->dataDir . '/mysql/ssl/ca.pem');

        $config = $this->getConfig(self::DRIVER);
        $dbConfig = $config['parameters']['db'];

        $dsn = sprintf(
            "mysql:host=%s;port=%s;dbname=%s;charset=utf8",
            $dbConfig['host'],
            $dbConfig['port'],
            $dbConfig['database']
        );

        $this->pdo = new PDO($dsn, $dbConfig['user'], $dbConfig['password'], $options);

        $this->pdo->setAttribute(PDO::MYSQL_ATTR_LOCAL_INFILE, true);
        $this->pdo->exec("SET NAMES utf8;");
    }

    protected function createAutoIncrementAndTimestampTable(): void
    {
        $this->pdo->exec('DROP TABLE IF EXISTS auto_increment_timestamp');

        $this->pdo->exec('CREATE TABLE auto_increment_timestamp (
            `_weird-I-d` INT NOT NULL AUTO_INCREMENT COMMENT \'This is a weird ID\',
            `weird-Name` VARCHAR(30) NOT NULL DEFAULT \'pam\' COMMENT \'This is a weird name\',
            `timestamp` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT \'This is a timestamp\',
            PRIMARY KEY (`_weird-I-d`)  
        ) COMMENT=\'This is a table comment\'');
        $this->pdo->exec('INSERT INTO auto_increment_timestamp (`weird-Name`) VALUES (\'george\'), (\'henry\')');
    }

    public function getConfig(string $driver = self::DRIVER, string $format = self::CONFIG_FORMAT_YAML): array
    {
        $config = parent::getConfig($driver, $format);
        if (!empty($config['parameters']['db']['#password'])) {
            $config['parameters']['db']['password'] = $config['parameters']['db']['#password'];
        }
        $config['parameters']['extractor_class'] = 'MySQL';
        return $config;
    }

    public function getConfigRow(string $driver = self::DRIVER): array
    {
        $config = parent::getConfigRow($driver);
        if (!empty($config['parameters']['db']['#password'])) {
            $config['parameters']['db']['password'] = $config['parameters']['db']['#password'];
        }
        $config['parameters']['extractor_class'] = 'MySQL';
        return $config;
    }

    protected function generateTableName(CsvFile $file): string
    {
        $tableName = sprintf(
            '%s',
            $file->getBasename('.' . $file->getExtension())
        );

        return $tableName;
    }

    protected function createTextTable(CsvFile $file, ?string $tableName = null, ?string $schemaName = null): void
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
    protected function countTable(CsvFile $file): int
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

    public function createApplication(array $config, array $state = []): MySQLApplication
    {
        $logger = new Logger('ex-db-mysql-tests');
        $app = new MySQLApplication($config, $logger, $state, $this->dataDir);

        return $app;
    }

    public function configTypesProvider(): array
    {
        return [
            [self::CONFIG_FORMAT_YAML],
            [self::CONFIG_FORMAT_JSON],
        ];
    }
}
