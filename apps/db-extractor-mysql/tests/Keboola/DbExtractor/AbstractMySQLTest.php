<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Csv\CsvReader;
use SplFileInfo;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Extractor\MySQL;
use Keboola\Component\Logger;
use Keboola\DbExtractor\MySQLApplication;
use Keboola\DbExtractor\Test\ExtractorTest;
use Symfony\Component\Filesystem\Filesystem;
use PDO;
use Symfony\Component\Process\Process;

abstract class AbstractMySQLTest extends ExtractorTest
{
    public const DRIVER = 'mysql';

    protected string $dataDir = __DIR__ . '/../../data';

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

        $options[PDO::MYSQL_ATTR_SSL_KEY] = realpath('/ssl-cert/client-key.pem');
        $options[PDO::MYSQL_ATTR_SSL_CERT] = realpath('/ssl-cert/client-cert.pem');
        $options[PDO::MYSQL_ATTR_SSL_CA] = realpath('/ssl-cert/ca.pem');
        $options[PDO::MYSQL_ATTR_SSL_CIPHER] = MySQL::SSL_CIPHER_CONFIG;

        $config = $this->getConfig(self::DRIVER);
        $dbConfig = $config['parameters']['db'];

        $dsn = sprintf(
            'mysql:host=%s;port=%s;dbname=%s;charset=utf8mb4',
            $dbConfig['host'],
            $dbConfig['port'],
            $dbConfig['database']
        );

        $this->pdo = new PDO($dsn, $dbConfig['user'], $dbConfig['#password'], $options);
        $this->pdo->setAttribute(PDO::MYSQL_ATTR_LOCAL_INFILE, true);
        $this->pdo->exec('SET NAMES utf8mb4;');
        $this->dropAllTables();
    }

    protected function tearDown(): void
    {
        parent::tearDown();

        $this->dropAllTables();

        # Close SSH tunnel if created
        $process = new Process(['sh', '-c', 'pgrep ssh | xargs -r kill']);
        $process->mustRun();
    }

    protected function dropAllTables(): void
    {
        $sql = <<<END
          SET FOREIGN_KEY_CHECKS = 0; 
          SET @tables = NULL;
          SET GROUP_CONCAT_MAX_LEN=32768;
        
          SELECT GROUP_CONCAT('`', table_schema, '`.`', table_name, '`') INTO @tables
          FROM   information_schema.tables 
          WHERE  TABLE_SCHEMA NOT IN ("performance_schema", "mysql", "information_schema", "sys");
          SELECT IFNULL(@tables, '') INTO @tables;
        
          SET        @tables = CONCAT('DROP TABLE IF EXISTS ', @tables);
          PREPARE    stmt FROM @tables;
          EXECUTE    stmt;
          DEALLOCATE PREPARE stmt;
          SET        FOREIGN_KEY_CHECKS = 1;
        END;

        $this->pdo->query($sql);
    }


    protected function createAutoIncrementAndTimestampTable(): void
    {
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

    protected function generateTableName(SplFileInfo $file): string
    {
        $tableName = sprintf(
            '%s',
            $file->getBasename('.' . $file->getExtension())
        );

        return $tableName;
    }

    protected function createTextTable(SplFileInfo $file, ?string $tableName = null, ?string $schemaName = null): void
    {
        if (!$tableName) {
            $tableName = $this->generateTableName($file);
        }

        if (!$schemaName) {
            $schemaName = 'test';
        } else {
            $this->pdo->exec(sprintf('CREATE DATABASE IF NOT EXISTS %s', $schemaName));
        }

        $csv = new CsvReader($file->getPathname());
        $this->pdo->exec(sprintf(
            'CREATE TABLE %s.%s (%s) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;',
            $schemaName,
            $tableName,
            implode(
                ', ',
                array_map(function ($column) {
                    return $column . ' text NULL';
                }, $csv->getHeader())
            )
        ));

        $query = "
			LOAD DATA LOCAL INFILE '{$file}'
			INTO TABLE `{$schemaName}`.`{$tableName}`
			CHARACTER SET utf8mb4
			FIELDS TERMINATED BY ','
			OPTIONALLY ENCLOSED BY '\"'
			ESCAPED BY ''
			IGNORE 1 LINES
		";

        $this->pdo->exec($query);

        $count = $this->pdo->query(sprintf('SELECT COUNT(*) AS itemsCount FROM %s.%s', $schemaName, $tableName))->fetchColumn();
        $this->assertEquals($this->countTable($csv), (int) $count);
    }

    protected function countTable(CsvReader $file): int
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
        $logger = new Logger();
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
                return [
                    0 =>
                        [
                            'name' => 'usergender',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                    1 =>
                        [
                            'name' => 'usercity',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                    2 =>
                        [
                            'name' => 'usersentiment',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                    3 =>
                        [
                            'name' => 'zipcode',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                    4 =>
                        [
                            'name' => 'sku',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                    5 =>
                        [
                            'name' => 'createdat',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                    6 =>
                        [
                            'name' => 'category',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                    7 =>
                        [
                            'name' => 'price',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                    8 =>
                        [
                            'name' => 'county',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                    9 =>
                        [
                            'name' => 'countycode',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                    10 =>
                        [
                            'name' => 'userstate',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                    11 =>
                        [
                            'name' => 'categorygroup',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                ];
            } else {
                throw new UserException(sprintf('Unexpected test table %s in schema %s', $table, $schema));
            }
        } else if ($schema === 'test') {
            switch ($table) {
                case 'sales':
                    return [
                        [
                            'name' => 'usergender',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'usercity',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'usersentiment',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'zipcode',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'sku',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'createdat',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'category',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'price',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'county',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'countycode',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'userstate',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'categorygroup',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                    ];
                case 'escaping':
                    return [
                        [
                            'name' => 'col1',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'col2',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                    ];
                case 'emoji':
                    return [
                        [
                            'name' => 'emoji',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                    ];
                case 'auto_increment_timestamp':
                    return [
                        [
                            'name' => '_weird-I-d',
                            'type' => 'int',
                            'primaryKey' => true,
                        ],
                        [
                            'name' => 'weird-Name',
                            'type' => 'varchar',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'timestamp',
                            'type' => 'timestamp',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'datetime',
                            'type' => 'datetime',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'intColumn',
                            'type' => 'int',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'decimalColumn',
                            'type' => 'decimal',
                            'primaryKey' => false,
                        ],
                    ];
            }
        }

        throw new UserException(sprintf('Unexpected schema %s', $schema));
    }
}
