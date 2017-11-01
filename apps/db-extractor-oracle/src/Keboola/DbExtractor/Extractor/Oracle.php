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
        $sql = <<<SQL_QUERY
          SELECT * FROM all_tables 
            JOIN (
              SELECT owner own, table_name tname FROM user_tab_privs WHERE privilege='SELECT'
                union 
                select rtp.owner own, rtp.table_name tname from user_role_privs urp, role_tab_privs rtp
                  where urp.granted_role = rtp.role and rtp.privilege='SELECT'
                union
                select user own, table_name tname from user_tables
            ) priv ON priv.own = all_tables.OWNER AND priv.tname = all_tables.TABLE_NAME
            WHERE all_tables.TABLESPACE_NAME != 'SYSAUX' AND all_tables.OWNER != 'SYS' AND all_tables.TABLESPACE_NAME != 'SYSTEM'
SQL_QUERY;

        if (!is_null($tables) && count($tables) > 0) {
            $sql .= sprintf(
                " AND all_tables.TABLE_NAME IN ('%s') AND all_tables.OWNER IN ('%s')",
                implode("','", array_map(function ($table) {
                    return $table['tableName'];
                }, $tables)),
                implode("','", array_map(function ($table) {
                    return $table['schema'];
                }, $tables))
            );
        }
        $sql .= " ORDER BY all_tables.OWNER, all_tables.TABLE_NAME";

        $stmt = oci_parse($this->db, $sql);

        $success = @oci_execute($stmt);
        if (!$success) {
            $error = oci_error($stmt);
            throw new UserException("Error attempting to list tables: " . $error['message']);
        }

        $numTables = oci_fetch_all($stmt, $resTables, 0, -1, OCI_FETCHSTATEMENT_BY_ROW);
        if ($numTables === 0) {
            return [];
        }

        $tableNameArray = [];
        $tableDefs = [];
        foreach ($resTables as $table) {
            $tableNameArray[] = $table['TABLE_NAME'];
            $tableDefs[$table['OWNER'] . '.' . $table['TABLE_NAME']] = [
                'name' => $table['TABLE_NAME'],
                'tablespaceName' => $table['TABLESPACE_NAME'],
                'schema' => $table['OWNER'],
                'owner' => $table['OWNER']
            ];
            if ($table['NUM_ROWS']) {
                $tabledefs[$table['OWNER'] . '.' . $table['TABLE_NAME']]['rowCount'] = $table['NUM_ROWS'];
            }
        }

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
            WHERE COLS.TABLE_NAME IN (%s) ORDER BY COLS.OWNER, COLS.TABLE_NAME, COLS.COLUMN_ID",
            implode(', ', array_map(function ($tableName) {
                return "'" . $tableName . "'";
            }, $tableNameArray))
        );

        $stmt = oci_parse($this->db, $sql);

        $success = @oci_execute($stmt);
        if (!$success) {
            $error = oci_error($stmt);
            throw new UserException("Error describing table {$table['TABLE_NAME']}: " . $error['message']);
        }

        $numrows = oci_fetch_all($stmt, $desc, 0, -1, OCI_FETCHSTATEMENT_BY_ROW);

        $previousOrdinalPos = 0;
        $previousTableName = '';
        foreach ($desc as $i => $column) {
            $curTable = $column['OWNER'] . '.' . $column['TABLE_NAME'];
            $length = $column['DATA_LENGTH'];
            if (!is_null($column['DATA_PRECISION'])  && !is_null($column['DATA_SCALE'])) {
                $length = $column['DATA_PRECISION'] . "," . $column['DATA_SCALE'];
            }
            if ($column['COLUMN_ID'] > $previousOrdinalPos || $previousTableName !== $curTable) {
                $curColumn = [
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
                        $curColumn['foreignKeyName'] = $column['CONSTRAINT_NAME'];
                        $curColumn['foreignKeyRefTable'] = $column['R_OWNER'];
                        $curColumn['foreignKeyRef'] = $column['R_CONSTRAINT_NAME'];
                        break;
                    case 'P':
                        $curColumn['primaryKey'] = true;
                        $curColumn['primaryKeyName'] = $column['CONSTRAINT_NAME'];
                        break;
                    case 'U':
                        $curColumn['uniqueKey'] = true;
                        $columns[$column['COLUMN_ID'] - 1]['uniqueKeyName'] = $column['CONSTRAINT_NAME'];
                        break;
                    default:
                        break;
                }
            }
            if (!array_key_exists('columns', $tableDefs[$curTable])) {
                $tableDefs[$curTable]['columns'] = [];
            }
            if ($column['COLUMN_ID'] > $previousOrdinalPos || $previousTableName !== $curTable) {
                $tableDefs[$curTable]['columns'][$column['COLUMN_ID'] - 1] = $curColumn;
            }
            if ($previousTableName !== $curTable) {
                $previousOrdinalPos = 0;
            } else {
                $previousOrdinalPos = $column['COLUMN_ID'];
            }
            $previousTableName = $curTable;
        }

        return array_values($tableDefs);
    }

    protected function describeTable(array $table)
    {
        // Deprecated
        return null;
    }

    public function simpleQuery(array $table, array $columns = array())
    {
        if (count($columns) > 0) {
            return sprintf("SELECT %s FROM %s.%s",
                implode(', ', array_map(function ($column) {
                    return $this->quote($column);
                }, $columns)),
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

    private function quote($obj) {
        return "\"{$obj}\"";
    }
}
