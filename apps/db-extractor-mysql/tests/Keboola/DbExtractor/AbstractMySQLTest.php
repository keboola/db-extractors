<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorLogger\Logger;
use Keboola\DbExtractor\MySQLApplication;
use Keboola\DbExtractor\Test\ExtractorTest;
use Symfony\Component\Filesystem\Filesystem;
use PDO;

abstract class AbstractMySQLTest extends ExtractorTest
{
    public const DRIVER = 'mysql';

    /** @var PDO */
    protected $pdo;

    public function setUp(): void
    {
        $this->dataDir = __DIR__ . '/../../data';

        $fs = new Filesystem();
        $fs->remove($this->dataDir . '/out/tables');
        $fs->mkdir($this->dataDir . '/out/tables');

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
            'mysql:host=%s;port=%s;dbname=%s;charset=utf8',
            $dbConfig['host'],
            $dbConfig['port'],
            $dbConfig['database']
        );

        $this->pdo = new PDO($dsn, $dbConfig['user'], $dbConfig['#password'], $options);

        $this->pdo->setAttribute(PDO::MYSQL_ATTR_LOCAL_INFILE, true);
        $this->pdo->exec('SET NAMES utf8;');
    }

    protected function createAutoIncrementAndTimestampTable(): void
    {
        $this->pdo->exec('DROP TABLE IF EXISTS auto_increment_timestamp_withFK');
        $this->pdo->exec('DROP TABLE IF EXISTS auto_increment_timestamp');

        $this->pdo->exec('CREATE TABLE auto_increment_timestamp (
            `_weird-I-d` INT NOT NULL AUTO_INCREMENT COMMENT \'This is a weird ID\',
            `weird-Name` VARCHAR(30) NOT NULL DEFAULT \'pam\' COMMENT \'This is a weird name\',
            `timestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT \'This is a timestamp\',
            `datetime` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT \'This is a datetime\',
            `intColumn` INT DEFAULT 1,
            `decimalColumn` DECIMAL(10,2) DEFAULT 10.2,
            PRIMARY KEY (`_weird-I-d`)  
        ) COMMENT=\'This is a table comment\'');
        $this->pdo->exec('INSERT INTO auto_increment_timestamp (`weird-Name`, `intColumn`, `decimalColumn`) VALUES (\'george\', 2, 20.2)');
        // Stagger the new column input timestamps
        sleep(1);
        $this->pdo->exec('INSERT INTO auto_increment_timestamp (`weird-Name`, `intColumn`, `decimalColumn`) VALUES (\'henry\', 3, 30.3)');
    }

    protected function createAutoIncrementAndTimestampTableWithFK(): void
    {
        $this->pdo->exec('DROP TABLE IF EXISTS auto_increment_timestamp_withFK');

        $this->pdo->exec('CREATE TABLE auto_increment_timestamp_withFK (
            `some_primary_key` INT NOT NULL AUTO_INCREMENT COMMENT \'This is a weird ID\',
            `random_name` VARCHAR(30) NOT NULL DEFAULT \'pam\' COMMENT \'This is a weird name\',
            `datetime` DATETIME DEFAULT CURRENT_TIMESTAMP,
            `foreign_key` INT COMMENT \'This is a foreign key\',
            PRIMARY KEY (`some_primary_key`),
            FOREIGN KEY (`foreign_key`) REFERENCES auto_increment_timestamp (`_weird-I-d`) ON DELETE CASCADE 
        ) COMMENT=\'This is a table comment\'');
        $this->pdo->exec('INSERT INTO auto_increment_timestamp_withFK (`random_name`, `foreign_key`) VALUES (\'sue\',1)');
    }

    public function getConfig(string $driver = self::DRIVER): array
    {
        $config = parent::getConfig($driver);
        $config['parameters']['extractor_class'] = 'MySQL';
        return $config;
    }

    public function getConfigRow(string $driver = self::DRIVER): array
    {
        $config = parent::getConfigRow($driver);
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
            $schemaName = 'test';
        } else {
            $this->pdo->exec(sprintf('CREATE DATABASE IF NOT EXISTS %s', $schemaName));
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

    public function configProvider(): array
    {
        $this->dataDir = __DIR__ . '/../../data';
        return [
            [$this->getConfig(self::DRIVER)],
            [$this->getConfigRow()],
        ];
    }

    protected function getIncrementalFetchingConfig(): array
    {
        $config = $this->getConfigRow(self::DRIVER);
        unset($config['parameters']['query']);
        $config['parameters']['table'] = [
            'tableName' => 'auto_increment_timestamp',
            'schema' => 'test',
        ];
        $config['parameters']['incremental'] = true;
        $config['parameters']['name'] = 'auto-increment-timestamp';
        $config['parameters']['outputTable'] = 'in.c-main.auto-increment-timestamp';
        $config['parameters']['primaryKey'] = ['_weird-I-d'];
        $config['parameters']['incrementalFetchingColumn'] = '_weird-I-d';
        return $config;
    }

    public function expectedTableColumns(string $schema, string $table): array
    {
        if ($schema === 'temp_schema') {
            if ($table === 'ext_sales') {
                return array (
                    0 =>
                        array (
                            'name' => 'usergender',
                            'sanitizedName' => 'usergender',
                            'type' => 'text',
                            'primaryKey' => false,
                            'length' => '65535',
                            'nullable' => true,
                            'ordinalPosition' => 1,
                            'uniqueKey' => false,
                        ),
                    1 =>
                        array (
                            'name' => 'usercity',
                            'sanitizedName' => 'usercity',
                            'type' => 'text',
                            'primaryKey' => false,
                            'length' => '65535',
                            'nullable' => true,
                            'ordinalPosition' => 2,
                            'uniqueKey' => false,
                        ),
                    2 =>
                        array (
                            'name' => 'usersentiment',
                            'sanitizedName' => 'usersentiment',
                            'type' => 'text',
                            'primaryKey' => false,
                            'length' => '65535',
                            'nullable' => true,
                            'ordinalPosition' => 3,
                            'uniqueKey' => false,
                        ),
                    3 =>
                        array (
                            'name' => 'zipcode',
                            'sanitizedName' => 'zipcode',
                            'type' => 'text',
                            'primaryKey' => false,
                            'length' => '65535',
                            'nullable' => true,
                            'ordinalPosition' => 4,
                            'uniqueKey' => false,
                        ),
                    4 =>
                        array (
                            'name' => 'sku',
                            'sanitizedName' => 'sku',
                            'type' => 'text',
                            'primaryKey' => false,
                            'length' => '65535',
                            'nullable' => true,
                            'ordinalPosition' => 5,
                            'uniqueKey' => false,
                        ),
                    5 =>
                        array (
                            'name' => 'createdat',
                            'sanitizedName' => 'createdat',
                            'type' => 'text',
                            'primaryKey' => false,
                            'length' => '65535',
                            'nullable' => true,
                            'ordinalPosition' => 6,
                            'uniqueKey' => false,
                        ),
                    6 =>
                        array (
                            'name' => 'category',
                            'sanitizedName' => 'category',
                            'type' => 'text',
                            'primaryKey' => false,
                            'length' => '65535',
                            'nullable' => true,
                            'ordinalPosition' => 7,
                            'uniqueKey' => false,
                        ),
                    7 =>
                        array (
                            'name' => 'price',
                            'sanitizedName' => 'price',
                            'type' => 'text',
                            'primaryKey' => false,
                            'length' => '65535',
                            'nullable' => true,
                            'ordinalPosition' => 8,
                            'uniqueKey' => false,
                        ),
                    8 =>
                        array (
                            'name' => 'county',
                            'sanitizedName' => 'county',
                            'type' => 'text',
                            'primaryKey' => false,
                            'length' => '65535',
                            'nullable' => true,
                            'ordinalPosition' => 9,
                            'uniqueKey' => false,
                        ),
                    9 =>
                        array (
                            'name' => 'countycode',
                            'sanitizedName' => 'countycode',
                            'type' => 'text',
                            'primaryKey' => false,
                            'length' => '65535',
                            'nullable' => true,
                            'ordinalPosition' => 10,
                            'uniqueKey' => false,
                        ),
                    10 =>
                        array (
                            'name' => 'userstate',
                            'sanitizedName' => 'userstate',
                            'type' => 'text',
                            'primaryKey' => false,
                            'length' => '65535',
                            'nullable' => true,
                            'ordinalPosition' => 11,
                            'uniqueKey' => false,
                        ),
                    11 =>
                        array (
                            'name' => 'categorygroup',
                            'sanitizedName' => 'categorygroup',
                            'type' => 'text',
                            'primaryKey' => false,
                            'length' => '65535',
                            'nullable' => true,
                            'ordinalPosition' => 12,
                            'uniqueKey' => false,
                        ),
                );
            } else {
                throw new UserException(sprintf('Unexpected test table %s in schema %s', $table, $schema));
            }
        } else if ($schema === 'test') {
            switch ($table) {
                case 'sales':
                    return array (
                        0 =>
                            array (
                                'name' => 'usergender',
                                'sanitizedName' => 'usergender',
                                'type' => 'text',
                                'primaryKey' => false,
                                'length' => '65535',
                                'nullable' => true,
                                'ordinalPosition' => 1,
                                'uniqueKey' => false,
                            ),
                        1 =>
                            array (
                                'name' => 'usercity',
                                'sanitizedName' => 'usercity',
                                'type' => 'text',
                                'primaryKey' => false,
                                'length' => '65535',
                                'nullable' => true,
                                'ordinalPosition' => 2,
                                'uniqueKey' => false,
                            ),
                        2 =>
                            array (
                                'name' => 'usersentiment',
                                'sanitizedName' => 'usersentiment',
                                'type' => 'text',
                                'primaryKey' => false,
                                'length' => '65535',
                                'nullable' => true,
                                'ordinalPosition' => 3,
                                'uniqueKey' => false,
                            ),
                        3 =>
                            array (
                                'name' => 'zipcode',
                                'sanitizedName' => 'zipcode',
                                'type' => 'text',
                                'primaryKey' => false,
                                'length' => '65535',
                                'nullable' => true,
                                'ordinalPosition' => 4,
                                'uniqueKey' => false,
                            ),
                        4 =>
                            array (
                                'name' => 'sku',
                                'sanitizedName' => 'sku',
                                'type' => 'text',
                                'primaryKey' => false,
                                'length' => '65535',
                                'nullable' => true,
                                'ordinalPosition' => 5,
                                'uniqueKey' => false,
                            ),
                        5 =>
                            array (
                                'name' => 'createdat',
                                'sanitizedName' => 'createdat',
                                'type' => 'text',
                                'primaryKey' => false,
                                'length' => '65535',
                                'nullable' => true,
                                'ordinalPosition' => 6,
                                'uniqueKey' => false,
                            ),
                        6 =>
                            array (
                                'name' => 'category',
                                'sanitizedName' => 'category',
                                'type' => 'text',
                                'primaryKey' => false,
                                'length' => '65535',
                                'nullable' => true,
                                'ordinalPosition' => 7,
                                'uniqueKey' => false,
                            ),
                        7 =>
                            array (
                                'name' => 'price',
                                'sanitizedName' => 'price',
                                'type' => 'text',
                                'primaryKey' => false,
                                'length' => '65535',
                                'nullable' => true,
                                'ordinalPosition' => 8,
                                'uniqueKey' => false,
                            ),
                        8 =>
                            array (
                                'name' => 'county',
                                'sanitizedName' => 'county',
                                'type' => 'text',
                                'primaryKey' => false,
                                'length' => '65535',
                                'nullable' => true,
                                'ordinalPosition' => 9,
                                'uniqueKey' => false,
                            ),
                        9 =>
                            array (
                                'name' => 'countycode',
                                'sanitizedName' => 'countycode',
                                'type' => 'text',
                                'primaryKey' => false,
                                'length' => '65535',
                                'nullable' => true,
                                'ordinalPosition' => 10,
                                'uniqueKey' => false,
                            ),
                        10 =>
                            array (
                                'name' => 'userstate',
                                'sanitizedName' => 'userstate',
                                'type' => 'text',
                                'primaryKey' => false,
                                'length' => '65535',
                                'nullable' => true,
                                'ordinalPosition' => 11,
                                'uniqueKey' => false,
                            ),
                        11 =>
                            array (
                                'name' => 'categorygroup',
                                'sanitizedName' => 'categorygroup',
                                'type' => 'text',
                                'primaryKey' => false,
                                'length' => '65535',
                                'nullable' => true,
                                'ordinalPosition' => 12,
                                'uniqueKey' => false,
                            ),
                    );
                case 'escaping':
                    return array (
                        0 =>
                            array (
                                'name' => 'col1',
                                'sanitizedName' => 'col1',
                                'type' => 'text',
                                'primaryKey' => false,
                                'length' => '65535',
                                'nullable' => true,
                                'ordinalPosition' => 1,
                                'uniqueKey' => false,
                            ),
                        1 =>
                            array (
                                'name' => 'col2',
                                'sanitizedName' => 'col2',
                                'type' => 'text',
                                'primaryKey' => false,
                                'length' => '65535',
                                'nullable' => true,
                                'ordinalPosition' => 2,
                                'uniqueKey' => false,
                            ),
                    );
                case 'auto_increment_timestamp':
                    return array (
                        0 =>
                            array (
                                'name' => '_weird-I-d',
                                'sanitizedName' => 'weird_I_d',
                                'type' => 'int',
                                'primaryKey' => true,
                                'length' => '10',
                                'nullable' => false,
                                'ordinalPosition' => 1,
                                'autoIncrement' => true,
                                'description' => 'This is a weird ID',
                                'uniqueKey' => false,
                                'autoIncrementValue' => 3,
                            ),
                        1 =>
                            array (
                                'name' => 'weird-Name',
                                'sanitizedName' => 'weird_Name',
                                'type' => 'varchar',
                                'primaryKey' => false,
                                'length' => '30',
                                'nullable' => false,
                                'default' => 'pam',
                                'ordinalPosition' => 2,
                                'description' => 'This is a weird name',
                                'uniqueKey' => false,
                            ),
                        2 =>
                            array (
                                'name' => 'timestamp',
                                'sanitizedName' => 'timestamp',
                                'type' => 'timestamp',
                                'primaryKey' => false,
                                'nullable' => false,
                                'default' => 'CURRENT_TIMESTAMP',
                                'ordinalPosition' => 3,
                                'description' => 'This is a timestamp',
                                'uniqueKey' => false,
                            ),
                        3 =>
                            array (
                                'name' => 'datetime',
                                'sanitizedName' => 'datetime',
                                'type' => 'datetime',
                                'primaryKey' => false,
                                'nullable' => false,
                                'default' => 'CURRENT_TIMESTAMP',
                                'ordinalPosition' => 4,
                                'description' => 'This is a datetime',
                                'uniqueKey' => false,
                            ),
                        4 =>
                            array (
                                'name' => 'intColumn',
                                'sanitizedName' => 'intColumn',
                                'type' => 'int',
                                'primaryKey' => false,
                                'length' => '10',
                                'nullable' => true,
                                'default' => '1',
                                'ordinalPosition' => 5,
                                'uniqueKey' => false,
                            ),
                        5 =>
                            array (
                                'name' => 'decimalColumn',
                                'sanitizedName' => 'decimalColumn',
                                'type' => 'decimal',
                                'primaryKey' => false,
                                'length' => '10,2',
                                'nullable' => true,
                                'default' => '10.20',
                                'ordinalPosition' => 6,
                                'uniqueKey' => false,
                            ),
                    );
            }
        } else {
            throw new UserException(sprintf('Unexpected schema %s', $schema));
        }
    }
}
