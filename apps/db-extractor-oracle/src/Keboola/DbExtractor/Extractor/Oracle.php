<?php

namespace Keboola\DbExtractor\Extractor;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Logger;

class Oracle extends Extractor
{
    protected $db;

    protected $dbParams;

    public function __construct($parameters, Logger $logger)
    {
        $this->dbParams = $parameters['db'];
        parent::__construct($parameters, $logger);
    }

    public function createConnection($params)
    {
        $dbString = '//' . $params['host'] . ':' . $params['port'] . '/' . $params['database'];
        $connection = oci_connect($params['user'], $params['password'], $dbString, 'AL32UTF8');

        if (!$connection) {
            $error = oci_error();
            throw new UserException("Error connection to DB: " . $error['message']);
        }
        return $connection;
    }

    public function createSshTunnel($dbConfig)
    {
        $this->dbParams = parent::createSshTunnel($dbConfig);
        return $this->dbParams;
    }

    protected function executeQuery($query, CsvFile $csv, $tableName): int
    {
        $sqlcl = new Sqlcl($this->dbParams, $this->logger);

        $linesWritten = $sqlcl->export($query, (string) $csv);
        if ($linesWritten === 0) {
            // remove the output file that only contains header
            @unlink((string) $csv);
        }
        return $linesWritten;
    }

    public function getConnection()
    {
        return $this->db;
    }

    public function testConnection()
    {
        $stmt = oci_parse($this->db, 'SELECT CURRENT_DATE FROM dual');
        $success = oci_execute($stmt);
        oci_free_statement($stmt);
        return $success;
    }

    public function getTables(array $tables = null)
    {
        $sql = <<<SQL
SELECT TABS.TABLE_NAME ,
    TABS.TABLESPACE_NAME ,
    TABS.OWNER ,
    TABS.NUM_ROWS ,
    COLS.COLUMN_NAME ,
    COLS.DATA_LENGTH ,
    COLS.DATA_PRECISION ,
    COLS.DATA_SCALE ,
    COLS.COLUMN_ID ,
    COLS.DATA_TYPE ,
    COLS.NULLABLE ,
    REFCOLS.CONSTRAINT_NAME ,
    REFCOLS.CONSTRAINT_TYPE ,
    REFCOLS.INDEX_NAME ,
    REFCOLS.R_CONSTRAINT_NAME,
    REFCOLS.R_OWNER
FROM ALL_TAB_COLUMNS COLS
    JOIN
    (
        SELECT 
        TABLE_NAME , 
        TABLESPACE_NAME, 
        OWNER , 
        NUM_ROWS
        FROM all_tables
        WHERE all_tables.TABLESPACE_NAME != 'SYSAUX'
        AND all_tables.TABLESPACE_NAME != 'SYSTEM'
        AND all_tables.OWNER != 'SYS'
        AND all_tables.OWNER != 'SYSTEM'
    )
    TABS
        ON COLS.TABLE_NAME = TABS.TABLE_NAME
        AND COLS.OWNER = TABS.OWNER
    LEFT OUTER JOIN
    (
        SELECT ACC.COLUMN_NAME ,
        ACC.TABLE_NAME ,
        AC.CONSTRAINT_NAME ,
        AC.R_CONSTRAINT_NAME,
        AC.INDEX_NAME ,
        AC.CONSTRAINT_TYPE ,
        AC.R_OWNER
        FROM ALL_CONS_COLUMNS ACC
            JOIN ALL_CONSTRAINTS AC
                ON ACC.CONSTRAINT_NAME = AC.CONSTRAINT_NAME
        WHERE AC.CONSTRAINT_TYPE IN ('P', 'U', 'R')
    )
    REFCOLS ON COLS.TABLE_NAME = REFCOLS.TABLE_NAME
        AND COLS.COLUMN_NAME = REFCOLS.COLUMN_NAME
SQL;

        $whereClause = "";
        if (!is_null($tables) && count($tables) > 0) {
            $whereClause = sprintf(
                " WHERE TABS.TABLE_NAME IN ('%s')",
                implode("','", array_map(function ($table) {
                    return $table['tableName'];
                }, $tables))
            );
        }

        $stmt = oci_parse($this->db, $sql . $whereClause);

        $success = oci_execute($stmt);
        if (!$success) {
            $error = oci_error($stmt);
            oci_free_statement($stmt);
            throw new UserException("Error fetching table listing: " . $error['message']);
        }

        $numrows = oci_fetch_all($stmt, $desc, 0, -1, OCI_FETCHSTATEMENT_BY_ROW);
        oci_free_statement($stmt);
        $tableDefs = [];
        foreach ($desc as $i => $column) {
            $curTable = $column['OWNER'] . '.' . $column['TABLE_NAME'];

            if (!array_key_exists($curTable, $tableDefs)) {
                $tableDefs[$curTable] = [
                    'name' => $column['TABLE_NAME'],
                    'tablespaceName' => $column['TABLESPACE_NAME'],
                    'schema' => $column['OWNER'],
                    'owner' => $column['OWNER']
                ];
                if ($column['NUM_ROWS']) {
                    $tabledefs[$curTable]['rowCount'] = $column['NUM_ROWS'];
                }
            }
            if (!array_key_exists('columns', $tableDefs[$curTable])) {
                $tableDefs[$curTable]['columns'] = [];
            }

            if (!array_key_exists($column['COLUMN_ID'] - 1, $tableDefs[$curTable]['columns'])) {
                $length = $column['DATA_LENGTH'];
                if (!is_null($column['DATA_PRECISION'])  && !is_null($column['DATA_SCALE'])) {
                    $length = $column['DATA_PRECISION'] . "," . $column['DATA_SCALE'];
                }
                $tableDefs[$curTable]['columns'][$column['COLUMN_ID'] - 1] = [
                    "name" => $column['COLUMN_NAME'],
                    "type" => $column['DATA_TYPE'],
                    "nullable" => ($column['NULLABLE'] === 'Y') ? true : false,
                    "length" => $length,
                    "ordinalPosition" => $column['COLUMN_ID'],
                    "primaryKey" => false,
                    "uniqueKey" => false
                ];
            }


            if (!is_null($column['CONSTRAINT_TYPE'])) {
                switch ($column['CONSTRAINT_TYPE']) {
                    case 'R':
                        $tableDefs[$curTable]['columns'][$column['COLUMN_ID'] - 1]['foreignKeyName'] = $column['CONSTRAINT_NAME'];
                        $tableDefs[$curTable]['columns'][$column['COLUMN_ID'] - 1]['foreignKeyRefTable'] = $column['R_OWNER'];
                        $tableDefs[$curTable]['columns'][$column['COLUMN_ID'] - 1]['foreignKeyRef'] = $column['R_CONSTRAINT_NAME'];
                        break;
                    case 'P':
                        $tableDefs[$curTable]['columns'][$column['COLUMN_ID'] - 1]['primaryKey'] = true;
                        $tableDefs[$curTable]['columns'][$column['COLUMN_ID'] - 1]['primaryKeyName'] = $column['CONSTRAINT_NAME'];
                        break;
                    case 'U':
                        $tableDefs[$curTable]['columns'][$column['COLUMN_ID'] - 1]['uniqueKey'] = true;
                        $tableDefs[$curTable]['columns'][$column['COLUMN_ID'] - 1]['uniqueKeyName'] = $column['CONSTRAINT_NAME'];
                        break;
                    default:
                        break;
                }
            }
        }
        ksort($tableDefs);
        return array_values($tableDefs);
    }

    public function simpleQuery(array $table, array $columns = array())
    {
        if (count($columns) > 0) {
            return sprintf(
                "SELECT %s FROM %s.%s",
                implode(
                    ', ',
                    array_map(
                        function ($column) {
                            return $this->quote($column);
                        },
                        $columns
                    )
                ),
                $this->quote($table['schema']),
                $this->quote($table['tableName'])
            );
        } else {
            return sprintf(
                "SELECT * FROM %s.%s",
                $this->quote($table['schema']),
                $this->quote($table['tableName'])
            );
        }
    }

    private function quote($obj)
    {
        return "\"{$obj}\"";
    }

    public function __destruct()
    {
        oci_close($this->db);
    }
}
