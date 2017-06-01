<?php
/**
 * @package ex-db-oracle
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\DbExtractor\Extractor;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Exception\UserException;

class Oracle extends Extractor
{
	protected $db;

	public function createConnection($params)
	{
		$dbString = '//' . $params['host'] . ':' . $params['port'] . '/' . $params['database'];
        $connection = @oci_connect($params['user'], $params['password'], $dbString, 'AL32UTF8');

        if (!$connection) {
            $error = oci_error();
            throw new UserException("Error connection to DB: " . $error['message']);
        }

		return $connection;
	}

	protected function executeQuery($query, CsvFile $csv)
	{
		$stmt = oci_parse($this->db, $query);
		$success = @oci_execute($stmt);

		if (!$success) {
			$error = oci_error($stmt);
			throw new UserException("Error executing query: " . $error['message']);
		}

        $resultRow = oci_fetch_assoc($stmt);
        if (!is_array($resultRow) || empty($resultRow)) {
            $this->logger->warn("Query returned empty result. Nothing was imported.");
            return 0;
        }

        // write header and first line
        $csv->writeRow(array_keys($resultRow));
        $csv->writeRow($resultRow);

        // write the rest
        $cnt = 1;
        while ($resultRow = oci_fetch_assoc($stmt)) {
            $csv->writeRow($resultRow);
            $cnt++;
        }

        return $cnt;
	}

	public function getConnection()
	{
		return $this->db;
	}

	public function testConnection()
	{
		$stmt = oci_parse($this->db, 'SELECT CURRENT_DATE FROM dual');
		return oci_execute($stmt);
	}

	public function listTables()
    {
        $stmt = oci_parse($this->db, "SELECT OWNER, TABLESPACE_NAME, TABLE_NAME  FROM all_tables WHERE TABLESPACE_NAME != 'SYSAUX' AND OWNER != 'SYS'");
        $success = @oci_execute($stmt);
        if (!$success) {
            $error = oci_error($stmt);
            throw new UserException("Error attempting to list tables: " . $error['message']);
        }

        $output = [];

        $numTables = oci_fetch_all($stmt, $tables, 0, -1, OCI_FETCHSTATEMENT_BY_ROW);
        echo "FOUND $numTables TABLES \n";
        foreach ($tables as $table) {
            var_dump($table);
            $output[] = $table['TABLE_NAME'];
        }
        return $output;
    }

    public function describeTable($tableName)
    {
        $sql = sprintf(
            "SELECT COLS.*, 
            REFCOLS.CONSTRAINT_NAME, REFCOLS.CONSTRAINT_TYPE, REFCOLS.INDEX_NAME, 
            REFCOLS.R_CONSTRAINT_NAME, REFCOLS.R_OWNER FROM ALL_TAB_COLUMNS COLS
            LEFT OUTER JOIN (
                SELECT  ACC.COLUMN_NAME, ACC.TABLE_NAME, AC.CONSTRAINT_NAME, 
                        AC.R_CONSTRAINT_NAME, AC.INDEX_NAME, AC.CONSTRAINT_TYPE, AC.R_OWNER
                FROM ALL_CONS_COLUMNS ACC
                JOIN ALL_CONSTRAINTS AC 
                ON ACC.CONSTRAINT_NAME = AC.CONSTRAINT_NAME
            ) REFCOLS ON COLS.TABLE_NAME = REFCOLS.TABLE_NAME AND COLS.COLUMN_NAME = REFCOLS.COLUMN_NAME
            WHERE COLS.TABLE_NAME = '%s'",
            $tableName
        );
        $stmt = oci_parse($this->db, $sql);

        $success = @oci_execute($stmt);
        if (!$success) {
            $error = oci_error($stmt);
            throw new UserException("Error describing table $tableName: " . $error['message']);
        }

        $numrows = oci_fetch_all($stmt, $desc, 0, -1, OCI_FETCHSTATEMENT_BY_ROW);

        $columns = [];
        foreach ($desc as $i => $column) {
            $length = $column['DATA_LENGTH'];
            if (!is_null($column['DATA_PRECISION'])  && !is_null($column['DATA_SCALE'])) {
                $length = $column['DATA_PRECISION'] . "," . $column['DATA_SCALE'];
            }
            $columns[$i] = [
                "name" => $column['COLUMN_NAME'],
                "type" => $column['DATA_TYPE'],
                "nullable" => ($column['NULLABLE'] === 'Y') ? true : false,
                "default" => $column['DATA_DEFAULT'],
                "length" => $length,
                "ordinalPosition" => $column['COLUMN_ID'],
            ];

            if (!is_null($column['CONSTRAINT_TYPE'])) {
                $columns[$i]['indexed'] = true;
                $columns[$i]['constraintName'] = $column['CONSTRAINT_NAME'];
                $columns[$i]['primaryKey'] = ($column['CONSTRAINT_TYPE'] === 'P') ? true : false;
                $columns[$i]['uniqueKey'] = ($column['CONSTRAINT_TYPE'] === 'U') ? true : false;
                if ($column['CONSTRAINT_TYPE'] === 'R') {
                    $columns[$i]['foreignKeyRefTable'] = $column['R_OWNER'];
                    $columns[$i]['foreignKeyRef'] = $column['R_CONSTRAINT_NAME'];
                }
            }
        }
        return $columns;
    }
}
