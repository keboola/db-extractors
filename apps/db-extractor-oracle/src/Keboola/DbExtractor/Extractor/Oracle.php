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

	public function getTables(array $tables = null)
    {

        $sql = "SELECT * FROM all_tables 
                        WHERE TABLESPACE_NAME != 'SYSAUX' AND TABLESPACE_NAME != 'SYSTEM' 
                        AND OWNER != 'SYS' AND OWNER != 'SYSTEM'";

        if (!is_null($tables) && count($tables) > 0) {
            $sql .= sprintf(
                " AND TABLE_NAME IN ('%s')",
                implode("','", array_map(function ($table) {
                    return $table;
                }, $tables))
            );
        }
        $sql .= $stmt = " ORDER BY TABLE_NAME";

        $stmt = oci_parse($this->db, $sql);

        $success = @oci_execute($stmt);
        if (!$success) {
            $error = oci_error($stmt);
            throw new UserException("Error attempting to list tables: " . $error['message']);
        }

        $output = [];
        $numTables = oci_fetch_all($stmt, $tables, 0, -1, OCI_FETCHSTATEMENT_BY_ROW);
        foreach ($tables as $table) {
            $output[] = $this->describeTable($table);
        }
        return $output;
    }

    protected function describeTable(array $table)
    {
        $tabledef = [
            'name' => $table['TABLE_NAME'],
            'schema' => $table['TABLESPACE_NAME'],
            'owner' => $table['OWNER'],
            'rowCount' => $table['NUM_ROWS']
        ];
        $sql = sprintf(
            "SELECT COLS.*, 
            REFCOLS.CONSTRAINT_NAME, REFCOLS.CONSTRAINT_TYPE, REFCOLS.INDEX_NAME, 
            REFCOLS.R_CONSTRAINT_NAME, REFCOLS.R_OWNER FROM ALL_TAB_COLUMNS COLS
            LEFT OUTER JOIN (
                SELECT  ACC.COLUMN_NAME, ACC.TABLE_NAME, AC.CONSTRAINT_NAME, 
                        AC.R_CONSTRAINT_NAME, AC.INDEX_NAME, AC.CONSTRAINT_TYPE, AC.R_OWNER
                FROM ALL_CONS_COLUMNS ACC
                JOIN ALL_CONSTRAINTS AC 
                ON ACC.CONSTRAINT_NAME = AC.CONSTRAINT_NAME WHERE AC.CONSTRAINT_TYPE IN ('P', 'U', 'R')
            ) REFCOLS ON COLS.TABLE_NAME = REFCOLS.TABLE_NAME AND COLS.COLUMN_NAME = REFCOLS.COLUMN_NAME
            WHERE COLS.TABLE_NAME = '%s' ORDER BY COLS.COLUMN_ID",
            $table['TABLE_NAME']
        );
        $stmt = oci_parse($this->db, $sql);

        $success = @oci_execute($stmt);
        if (!$success) {
            $error = oci_error($stmt);
            throw new UserException("Error describing table {$table['TABLE_NAME']}: " . $error['message']);
        }

        $numrows = oci_fetch_all($stmt, $desc, 0, -1, OCI_FETCHSTATEMENT_BY_ROW);

        $columns = [];
        $previousOrdinalPos = 0;
        foreach ($desc as $i => $column) {
            $length = $column['DATA_LENGTH'];
            if (!is_null($column['DATA_PRECISION'])  && !is_null($column['DATA_SCALE'])) {
                $length = $column['DATA_PRECISION'] . "," . $column['DATA_SCALE'];
            }

            if ($column['COLUMN_ID'] > $previousOrdinalPos) {
                $columns[$column['COLUMN_ID'] - 1] = [
                    "name" => $column['COLUMN_NAME'],
                    "type" => $column['DATA_TYPE'],
                    "nullable" => ($column['NULLABLE'] === 'Y') ? true : false,
                    "default" => $column['DATA_DEFAULT'],
                    "length" => $length,
                    "ordinalPosition" => $column['COLUMN_ID'],
                    "primaryKey" => false,
                    "uniqueKey" => false
                ];
            }

            if (!is_null($column['CONSTRAINT_TYPE'])) {
                switch ($column['CONSTRAINT_TYPE']) {
                    case 'R':
                        $columns[$column['COLUMN_ID'] - 1]['foreignKeyName'] = $column['CONSTRAINT_NAME'];
                        $columns[$column['COLUMN_ID'] - 1]['foreignKeyRefTable'] = $column['R_OWNER'];
                        $columns[$column['COLUMN_ID'] - 1]['foreignKeyRef'] = $column['R_CONSTRAINT_NAME'];
                        break;
                    case 'P':
                        $columns[$column['COLUMN_ID'] - 1]['primaryKey'] = true;
                        $columns[$column['COLUMN_ID'] - 1]['primaryKeyName'] = $column['CONSTRAINT_NAME'];
                        break;
                    case 'U':
                        $columns[$column['COLUMN_ID'] - 1]['uniqueKey'] = true;
                        $columns[$column['COLUMN_ID'] - 1]['uniqueKeyName'] = $column['CONSTRAINT_NAME'];
                        break;
                    default:
                        break;
                }
            }
            $previousOrdinalPos = $column['COLUMN_ID'];
        }
        $tabledef['columns'] = $columns;

        return $tabledef;
    }
}
