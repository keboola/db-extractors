<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\MSSQLApplication;
use Keboola\DbExtractor\Test\ExtractorTest;
use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Logger;

abstract class OracleBaseTest extends ExtractorTest
{
    protected $connection;

    public const DRIVER = 'oracle';

    /** @var string */
    protected $dataDir = __DIR__ . '/../../data';

    public function setUp(): void
    {
        if (!defined('APP_NAME')) {
            define('APP_NAME', 'ex-db-oracle');
        }

        $config = $this->getConfig('oracle');
        $dbConfig = $config['parameters']['db'];
        $dbString = '//' . $dbConfig['host'] . ':' . $dbConfig['port'] . '/' . $dbConfig['database'];

        $adminConnection = @oci_connect('system', 'oracle', $dbString, 'AL32UTF8');
        if (!$adminConnection) {
            $error = oci_error();
            echo "ADMIN CONNECTION ERROR: " . $error['message'];
        }
        try {
            // create test user
            $stmt = oci_parse(
                $adminConnection,
                sprintf("CREATE USER %s IDENTIFIED BY %s DEFAULT TABLESPACE users", $dbConfig['user'], $dbConfig['#password'])
            );
            oci_execute($stmt);
            oci_free_statement($stmt);

            // provide roles
            $stmt = oci_parse(
                $adminConnection,
                sprintf("GRANT CONNECT,RESOURCE,DBA TO %s", $dbConfig['user'])
            );
            oci_execute($stmt);
            oci_free_statement($stmt);

            // grant privileges
            $stmt = oci_parse(
                $adminConnection,
                sprintf("GRANT CREATE SESSION GRANT ANY PRIVILEGE TO %s", $dbConfig['user'])
            );
            oci_execute($stmt);
            oci_free_statement($stmt);
        } catch (\Exception $e) {
            // make sure this is the case that TESTER already exists
            if (!strstr($e->getMessage(), "ORA-01920")) {
                throw $e;
            }
        }
        if ($adminConnection) {
            oci_close($adminConnection);
        }
        $this->connection = oci_connect($dbConfig['user'], $dbConfig['#password'], $dbString, 'AL32UTF8');

        // drop the clob test table
        $stmt = oci_parse($this->connection, "DROP TABLE CLOB_TEST");
        try {
            oci_execute($stmt);
        } catch (\Exception $e) {
            // table doesn't exists
        } finally {
            oci_free_statement($stmt);
        }
    }

    public function tearDown()
    {
        if ($this->connection) {
            oci_close($this->connection);
        }
        parent::tearDown();
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
    protected function createTextTable(CsvFile $file, $primaryKey = [])
    {
        $tableName = $this->generateTableName($file);

        try {
            oci_execute(oci_parse($this->connection, sprintf("DROP TABLE %s", $tableName)));
        } catch (\Exception $e) {
            // table dont exists
        }

        $header = $file->getHeader();

        oci_execute(oci_parse($this->connection, sprintf(
            'CREATE TABLE %s (%s) tablespace users',
            $tableName,
            implode(
                ', ',
                array_map(function ($column) {
                    return $column . ' NVARCHAR2 (400)';
                }, $header)
            )
        )));

        // create the primary key if supplied
        if ($primaryKey && is_array($primaryKey) && !empty($primaryKey)) {
            foreach ($primaryKey as $pk) {
                oci_execute(
                    oci_parse(
                        $this->connection,
                        sprintf("ALTER TABLE %s MODIFY %s NVARCHAR2(64) NOT NULL", $tableName, $pk)
                    )
                );
            }
            oci_execute(oci_parse(
                $this->connection,
                sprintf(
                    'ALTER TABLE %s ADD CONSTRAINT PK_%s PRIMARY KEY (%s)',
                    $tableName,
                    $tableName,
                    implode(',', $primaryKey)
                )
            ));
        }

        $file->next();

        $columnsCount = count($file->current());
        $rowsPerInsert = intval((1000 / $columnsCount) - 1);

        while ($file->current() != false) {
            for ($i=0; $i<$rowsPerInsert && $file->current() !== false; $i++) {
                $cols = [];
                foreach ($file->current() as $col) {
                    $cols[] = "'" . $col . "'";
                }
                $sql = sprintf(
                    "INSERT INTO {$tableName} (%s) VALUES (%s)",
                    implode(',', $header),
                    implode(',', $cols)
                );

                oci_execute(oci_parse($this->connection, $sql));

                $file->next();
            }
        }

        $stmt = oci_parse($this->connection, sprintf('SELECT COUNT(*) AS ITEMSCOUNT FROM %s', $tableName));
        oci_execute($stmt);
        $row = oci_fetch_assoc($stmt);

        $this->assertEquals($this->countTable($file), (int) $row['ITEMSCOUNT']);
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
}
