<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 10/02/16
 * Time: 17:49
 */

namespace Keboola\DbExtractor\Extractor;

use Keboola\Csv\CsvFile;
use Keboola\Csv\Exception;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;

class Redshift extends Extractor
{
    private $dbConfig;

    public function createConnection($dbParams)
    {
        $this->dbConfig = $dbParams;

        // check params
        foreach (['host', 'database', 'user', 'password'] as $r) {
            if (!isset($dbParams[$r])) {
                throw new UserException(sprintf("Parameter %s is missing.", $r));
            }
        }

        $port = isset($dbParams['port']) ? $dbParams['port'] : '5439';

        $pdo = new \PDO(
            "pgsql:dbname={$dbParams['database']};port={$port};host=" . $dbParams['host'],
            $dbParams['user'],
            $dbParams['password']
        );
        $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        return $pdo;
    }

    private function restartConnection()
    {
        $this->db = null;
        try {
            $this->db = $this->createConnection($this->dbConfig);
        } catch (\Exception $e) {
            throw new UserException(sprintf("Error connecting to DB: %s", $e->getMessage()), 0, $e);
        }
    }

    public function export(array $table)
    {
        $outputTable = $table['outputTable'];
        $csv = $this->createOutputCsv($outputTable);
        $this->logger->info("Exporting to " . $outputTable);
        if (isset($table['query']) && strlen($table['query']) > 0) {
            $query = $table['query'];
        } else {
            $query = $this->simpleQuery($table['table'], $table['columns']);
        }
        $tries = 0;
        $exception = null;
        $csvCreated = false;
        while ($tries < 5) {
            $exception = null;
            try {
                if ($tries > 0) {
                    $this->restartConnection();
                }
                $csvCreated = $this->executeQuery($query, $csv);
                break;
            } catch (\PDOException $e) {
                $exception = new UserException("DB query failed: " . $e->getMessage(), 0, $e);
            }
            sleep(pow($tries, 2));
            $tries++;
        }
        if ($exception) {
            throw $exception;
        }
        if ($csvCreated) {
            if ($this->createManifest($table) === false) {
                throw new ApplicationException("Unable to create manifest", 0, null, [
                    'table' => $table
                ]);
            }
        }
        return $outputTable;

    }

    protected function executeQuery($query, CsvFile $csv)
    {
        $statement = $this->db->query($query);

        if ($statement === FALSE) {
            throw new UserException("Failed to execute the provided query.");
        }

        $i = 0;
        while ($row = $statement->fetch(\PDO::FETCH_ASSOC)) {
            if ($i === 0) {
                $csv->writeRow(array_keys($row));
            }
            $csv->writeRow($row);
            $i++;
        }

        return ($i > 0);
    }

    public function testConnection()
    {
        $this->db->query("SELECT 1");
    }


    public function getTables(array $tables = null)
    {
        $sql = "SELECT * FROM information_schema.tables 
                WHERE table_schema != 'pg_catalog' AND table_schema != 'information_schema'";

        if (!is_null($tables) && count($tables) > 0) {
            $sql .= sprintf(
                " AND table_name IN (%s) AND table_schema IN (%s)",
                implode(',', array_map(function ($table) {
                    return $this->db->quote($table['tableName']);
                }, $tables)),
                implode(',', array_map(function ($table) {
                    return $this->db->quote($table['schema']);
                }, $tables))
            );
        }

        $res = $this->db->query($sql);
        $arr = $res->fetchAll();

        $output = [];
        foreach ($arr as $table) {
            $output[] = $this->describeTable($table);
        }
        return $output;
    }

    public function describeTable(array $table)
    {
        $tabledef = [
            'name' => $table['table_name'],
            'schema' => (isset($table['table_schema'])) ? $table['table_schema'] : null,
            'type' => (isset($table['table_type'])) ? $table['table_type'] : null,
            'catalog' => (isset($table['table_catalog'])) ? $table['table_catalog'] : null
        ];

        $sql = "
            SELECT cols.column_name, cols.column_default, cols.is_nullable, cols.data_type, cols.ordinal_position,
                        cols.character_maximum_length, cols.numeric_precision, cols.numeric_scale,
                        def.contype, def.conkey
            FROM information_schema.columns as cols 
            JOIN (
              SELECT
                a.attnum,
                n.nspname,
                c.relname,
                a.attname AS colname,
                t.typname AS type,
                a.atttypmod,
                FORMAT_TYPE(a.atttypid, a.atttypmod) AS complete_type,
                d.adsrc AS default_value,
                a.attnotnull AS notnull,
                a.attlen AS length,
                co.contype,
                ARRAY_TO_STRING(co.conkey, ',') AS conkey
              FROM pg_attribute AS a
                JOIN pg_class AS c ON a.attrelid = c.oid
                JOIN pg_namespace AS n ON c.relnamespace = n.oid
                JOIN pg_type AS t ON a.atttypid = t.oid
                LEFT OUTER JOIN pg_constraint AS co ON (co.conrelid = c.oid
                    AND a.attnum = ANY(co.conkey) AND (co.contype = 'p' OR co.contype = 'u'))
                LEFT OUTER JOIN pg_attrdef AS d ON d.adrelid = c.oid AND d.adnum = a.attnum
              WHERE a.attnum > 0 AND c.relname = " . $this->db->quote($tabledef['name']) . "
            ) as def 
            ON cols.column_name = def.colname
            WHERE cols.table_name = " . $this->db->quote($tabledef['name']);
        if ($tabledef['schema']) {
            $sql .= " AND cols.table_schema = " . $this->db->quote($tabledef['schema']);
        }
        $sql .= ' ORDER BY cols.ordinal_position';

        $res = $this->db->query($sql);

        $rows = $res->fetchAll(\PDO::FETCH_ASSOC);
        
        $columns = [];
        foreach ($rows as $i => $column) {

            $length = ($column['character_maximum_length']) ? $column['character_maximum_length'] : null;
            if (is_null($length) && !is_null($column['numeric_precision'])) {
                if ($column['numeric_scale'] > 0) {
                    $length = $column['numeric_precision'] . "," . $column['numeric_scale'];
                } else {
                    $length = $column['numeric_precision'];
                }
            }
            $default = null;
            if (!is_null($column['column_default'])) {
                $default = str_replace("'", "", explode('::', $column['column_default'])[0]);
            }
            $columns[$i] = [
                "name" => $column['column_name'],
                "type" => $column['data_type'],
                "primaryKey" => ($column['contype'] === "p") ? true : false,
                "uniqueKey" => ($column['contype'] === "u") ? true : false,
                "length" => $length,
                "nullable" => ($column['is_nullable'] === "NO") ? false : true,
                "default" => $default,
                "ordinalPosition" => $column['ordinal_position']
            ];
        }

        $tabledef['columns'] = $columns;
        return $tabledef;
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

    private function quote($obj)
    {
        $q = '"';
        return ($q . str_replace("$q", "$q$q", $obj) . $q);
    }
}
