<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Exception;
use Keboola\DbExtractorLogger\Logger;
use Keboola\DbExtractor\OracleApplication;
use Keboola\DbExtractor\Test\ExtractorTest;
use Keboola\Csv\CsvFile;

abstract class OracleBaseTest extends ExtractorTest
{
    /** @var mixed */
    protected $connection;

    /** @var string  */
    protected $dataDir = __DIR__ . '/../../data';

    public const DRIVER = 'oracle';

    public function setUp(): void
    {
        $config = $this->getConfig('oracle');
        // write configuration file for exporter
        file_put_contents($this->dataDir . '/config.json', json_encode($config));
        $dbConfig = $config['parameters']['db'];
        $dbString = '//' . $dbConfig['host'] . ':' . $dbConfig['port'] . '/' . $dbConfig['database'];

        $adminConnection = @oci_connect('system', 'oracle', $dbString, 'AL32UTF8');
        if (!$adminConnection) {
            $error = oci_error();
            echo 'ADMIN CONNECTION ERROR: ' . $error['message'];
        } else {
            try {
                // create test user
                $this->executeStatement(
                    $adminConnection,
                    sprintf(
                        'CREATE USER %s IDENTIFIED BY %s DEFAULT TABLESPACE users',
                        $dbConfig['user'],
                        $dbConfig['#password']
                    )
                );

                // provide roles
                $this->executeStatement(
                    $adminConnection,
                    sprintf('GRANT CONNECT,RESOURCE,DBA TO %s', $dbConfig['user'])
                );

                // grant privileges
                $this->executeStatement(
                    $adminConnection,
                    sprintf('GRANT CREATE SESSION GRANT ANY PRIVILEGE TO %s', $dbConfig['user'])
                );
            } catch (\Throwable $e) {
                // make sure this is the case that TESTER already exists
                if (!strstr($e->getMessage(), 'ORA-01920')) {
                    echo "\nCreate test user error: " . $e->getMessage() . "\n";
                    echo "\nError code: " . $e->getCode() . "\n";
                }
            }
        }
        if ($adminConnection) {
            oci_close($adminConnection);
        }
        $this->connection = oci_connect($dbConfig['user'], $dbConfig['#password'], $dbString, 'AL32UTF8');
        $this->setupTestTables();
        $this->createClobTable();
        $this->createRegionsTable();
        $this->cleanupOutputDirectory();
    }

    public function tearDown(): void
    {
        if ($this->connection) {
            oci_close($this->connection);
        }
        parent::tearDown();
    }

    private function cleanupOutputDirectory(): void
    {
        if (file_exists($this->dataDir . '/out/tables')) {
            $dh = opendir($this->dataDir . '/out/tables');
            if ($dh) {
                while (false !== ($file = readdir($dh))) {
                    @unlink($file);
                }
            }
        }
    }

    /**
     * @param array $config
     * @return OracleApplication
     */
    public function createApplication(array $config, array $state = []): OracleApplication
    {
        $logger = new Logger('ex-db-mysql-tests');
        return new OracleApplication($config, $logger, $state, $this->dataDir);
    }

    /**
     * @param mixed $connection
     * @param string $sql
     * @throws \Throwable
     */
    protected function executeStatement($connection, string $sql): void
    {
        $stmt = oci_parse($connection, $sql);
        if (!$stmt) {
            throw new Exception(sprintf('Can\'t parse sql statement %s', $sql));
        }
        try {
            oci_execute($stmt);
        } catch (\Throwable $e) {
            throw $e;
        } finally {
            oci_free_statement($stmt);
        }
    }

    protected function setupTestTables(): void
    {
        $csv1 = new CsvFile($this->dataDir . '/oracle/sales.csv');
        $this->createTextTable($csv1, ['CREATEDAT']);

        $csv2 = new CsvFile($this->dataDir . '/oracle/escaping.csv');
        $this->createTextTable($csv2);
    }

    protected function setupTestRowTable(): void
    {
        $csv1 = new CsvFile($this->dataDir . '/oracle/sales.csv');
        $this->createTextTable($csv1, ['CREATEDAT']);
    }

    protected function generateTableName(CsvFile $file): string
    {
        $tableName = $file->getBasename('.' . $file->getExtension());
        return $tableName;
    }

    private function dropTableIfExists(string $tablename): void
    {
        $sql = <<<EOT
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE %s';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;
EOT;
        $this->executeStatement(
            $this->connection,
            sprintf($sql, $tablename)
        );
    }

    private function dropSequenceIfExists(string $sequenceName): void
    {
        $sql = <<<EOT
BEGIN
   EXECUTE IMMEDIATE 'DROP SEQUENCE %s';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -2289 THEN
         RAISE;
      END IF;
END;
EOT;
        $this->executeStatement(
            $this->connection,
            sprintf($sql, $sequenceName)
        );
    }

    private function createAutoIncrement(string $tableName, string $incrementColumn): void
    {
        $lowerTableName = strtolower($tableName);
        $sequenceName = $lowerTableName . '_seq';
        $this->dropSequenceIfExists($sequenceName);

        $sqlSequence = 'CREATE SEQUENCE %s START WITH 1';

        $sqlTrigger = <<<EOT
CREATE OR REPLACE TRIGGER %s_on_insert
BEFORE INSERT ON "%s"
FOR EACH ROW
BEGIN
  SELECT %s_seq.NEXTVAL
  INTO   :new."%s"
  FROM   dual;
END;
EOT;
        $this->executeStatement(
            $this->connection,
            sprintf($sqlSequence, $sequenceName)
        );

        $this->executeStatement(
            $this->connection,
            sprintf(
                $sqlTrigger,
                substr($lowerTableName, 0, 20),
                $tableName,
                $lowerTableName,
                $incrementColumn
            )
        );
    }

    protected function createClobTable(): void
    {
        // drop the
        $this->dropTableIfExists('CLOB_TEST');

        // create the clob table
        $this->executeStatement(
            $this->connection,
            'CREATE TABLE CLOB_TEST (id VARCHAR(25), clob_col CLOB) tablespace users'
        );
        $this->executeStatement(
            $this->connection,
            "INSERT INTO CLOB_TEST VALUES ('hello', '<test>some test xml </test>')"
        );
        $this->executeStatement(
            $this->connection,
            "INSERT INTO CLOB_TEST VALUES ('nullTest', null)"
        );
        $this->executeStatement(
            $this->connection,
            "INSERT INTO CLOB_TEST VALUES ('goodbye', '<test>some test xml </test>')"
        );
    }

    protected function createRegionsTable(): void
    {
        $this->dropTableIfExists('REGIONS');

        $this->executeStatement(
            $this->connection,
            'CREATE TABLE REGIONS AS SELECT * FROM HR.REGIONS'
        );

        $this->executeStatement(
            $this->connection,
            'ALTER TABLE REGIONS DROP COLUMN REGION_NAME'
        );
    }

    public function createIncrementalFetchingTable(array $config): void
    {
        $this->dropTableIfExists($config['parameters']['table']['tableName']);

        $createTableQuery =
            'CREATE TABLE %s (' .
            '"id" INT NOT NULL,' .
            '"name" NVARCHAR2(400),' .
            '"decimal" DECIMAL(10,8),' .
            '"date" DATE,' .
            '"timestamp" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,' .
            'CONSTRAINT pk PRIMARY KEY ("id")' .
            ') tablespace users'
        ;
        $this->executeStatement(
            $this->connection,
            sprintf(
                $createTableQuery,
                $config['parameters']['table']['tableName']
            )
        );

        $this->createAutoIncrement($config['parameters']['table']['tableName'], 'id');

        $this->executeStatement(
            $this->connection,
            sprintf(
                'INSERT INTO %s ("name", "decimal", "date")' .
                ' VALUES (\'william\', 10.7, TO_DATE(\'2019-11-20\', \'yyyy-mm-dd\'))',
                $config['parameters']['table']['tableName']
            )
        );

        sleep(1);
        $this->executeStatement(
            $this->connection,
            sprintf(
                'INSERT INTO %s ("name", "decimal", "date")' .
                ' VALUES (\'charles\', 38.9827423, TO_DATE(\'2019-11-21\', \'yyyy-mm-dd\'))',
                $config['parameters']['table']['tableName']
            )
        );
    }

    /**
     * Create table from csv file with text columns
     *
     * @param CsvFile $file
     */
    protected function createTextTable(CsvFile $file, array $primaryKey = []): void
    {
        $tableName = $this->generateTableName($file);

        $this->dropTableIfExists($tableName);

        $header = $file->getHeader();

        $createTableStatement = sprintf(
            'CREATE TABLE %s (%s) tablespace users',
            $tableName,
            implode(
                ', ',
                array_map(function ($column) {
                    return '"' . $column . '" NVARCHAR2 (400)';
                }, $header)
            )
        );

        $this->executeStatement(
            $this->connection,
            $createTableStatement
        );

        // create the primary key if supplied
        if ($primaryKey && is_array($primaryKey) && !empty($primaryKey)) {
            foreach ($primaryKey as $pk) {
                $this->executeStatement(
                    $this->connection,
                    sprintf('ALTER TABLE %s MODIFY %s NVARCHAR2(64) NOT NULL', $tableName, $pk)
                );
            }
            $this->executeStatement(
                $this->connection,
                sprintf(
                    'ALTER TABLE %s ADD CONSTRAINT PK_%s PRIMARY KEY (%s)',
                    $tableName,
                    $tableName,
                    implode(',', $primaryKey)
                )
            );
        }

        $file->next();

        $columnsCount = count($file->current());
        $rowsPerInsert = intval((1000 / $columnsCount) - 1);

        while ($file->current() !== false) {
            for ($i=0; $i<$rowsPerInsert && $file->current() !== false; $i++) {
                $cols = [];
                foreach ($file->current() as $col) {
                    $cols[] = "'" . $col . "'";
                }
                $sql = sprintf(
                    "INSERT INTO {$tableName} (\"%s\") VALUES (%s)",
                    implode('","', $header),
                    implode(',', $cols)
                );

                $this->executeStatement($this->connection, $sql);

                $file->next();
            }
        }

        $sql = sprintf('SELECT COUNT(*) AS ITEMSCOUNT FROM %s', $tableName);
        $stmt = oci_parse($this->connection, $sql);
        if (!$stmt) {
            throw new Exception(sprintf('Can\'t parse sql statement %s', $sql));
        }
        oci_execute($stmt);
        $row = oci_fetch_assoc($stmt);
        oci_free_statement($stmt);
        $this->assertEquals($this->countTable($file), (int) $row['ITEMSCOUNT']);
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

    public function getPrivateKey(): string
    {
        return (string) file_get_contents('/root/.ssh/id_rsa');
    }

    public function getPublicKey(): string
    {
        return (string) file_get_contents('/root/.ssh/id_rsa.pub');
    }
}
