<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Csv\CsvFile;
use Keboola\Csv\Exception;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;

class Redshift extends Extractor
{
    public function createConnection(array $dbParams): \PDO
    {
        // check params
        foreach (['host', 'database', 'user', 'password'] as $r) {
            if (!isset($dbParams[$r])) {
                throw new UserException(sprintf('Parameter %s is missing.', $r));
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

    public function testConnection(): void
    {
        $this->db->query('SELECT 1');
    }


    public function getTables(?array $tables = null): array
    {
        $sql = "SELECT * FROM information_schema.tables 
                WHERE table_schema != 'pg_catalog' AND table_schema != 'information_schema'";

        if (!is_null($tables) && count($tables) > 0) {
            $sql .= sprintf(
                ' AND table_name IN (%s) AND table_schema IN (%s)',
                implode(',', array_map(function ($table) {
                    return $this->db->quote($table['tableName']);
                }, $tables)),
                implode(',', array_map(function ($table) {
                    return $this->db->quote($table['schema']);
                }, $tables))
            );
        }

        $sql .= ' ORDER BY table_schema, table_name';

        $res = $this->db->query($sql);
        $arr = $res->fetchAll();

        if (count($arr) === 0) {
            return [];
        }

        $tableNameArray = [];
        $tableDefs = [];
        foreach ($arr as $table) {
            $tableNameArray[] = $table['table_name'];
            $tableDefs[$table['table_schema'] . '.' . $table['table_name']] = [
                'name' => $table['table_name'],
                'schema' => (isset($table['table_schema'])) ? $table['table_schema'] : null,
                'type' => (isset($table['table_type'])) ? $table['table_type'] : null,
                'catalog' => (isset($table['table_catalog'])) ? $table['table_catalog'] : null,
            ];
        }

        $sql = sprintf(
            "
            SELECT cols.column_name, cols.table_name, cols.table_schema, 
                    cols.column_default, cols.is_nullable, cols.data_type, cols.ordinal_position,
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
              WHERE a.attnum > 0 AND c.relname IN (%s)
            ) as def 
            ON cols.column_name = def.colname AND cols.table_name = def.relname
            WHERE cols.table_name IN (%s) ORDER BY cols.table_schema, cols.table_name, cols.ordinal_position",
            implode(', ', array_map(function ($tableName) {
                return $this->db->quote($tableName);
            }, $tableNameArray)),
            implode(', ', array_map(function ($tableName) {
                return $this->db->quote($tableName);
            }, $tableNameArray))
        );
        $res = $this->db->query($sql);
        $rows = $res->fetchAll(\PDO::FETCH_ASSOC);

        $columns = [];
        foreach ($rows as $i => $column) {
            $curTable = $column['table_schema'] . '.' . $column['table_name'];
            $length = ($column['character_maximum_length']) ? $column['character_maximum_length'] : null;
            if (is_null($length) && !is_null($column['numeric_precision'])) {
                if ($column['numeric_scale'] > 0) {
                    $length = $column['numeric_precision'] . ',' . $column['numeric_scale'];
                } else {
                    $length = $column['numeric_precision'];
                }
            }
            $default = null;
            if (!is_null($column['column_default'])) {
                $default = str_replace("'", '', explode('::', $column['column_default'])[0]);
            }
            $curColumn = [
                'name' => $column['column_name'],
                'type' => $column['data_type'],
                'primaryKey' => ($column['contype'] === 'p') ? true : false,
                'uniqueKey' => ($column['contype'] === 'u') ? true : false,
                'length' => $length,
                'nullable' => ($column['is_nullable'] === 'NO') ? false : true,
                'default' => $default,
                'ordinalPosition' => $column['ordinal_position'],
            ];
            if (!array_key_exists('columns', $tableDefs[$curTable])) {
                $tableDefs[$curTable]['columns'] = [];
            }
            $tableDefs[$curTable]['columns'][] = $curColumn;
        }
        return array_values($tableDefs);
    }

    public function simpleQuery(array $table, array $columns = array()): string
    {
        if (count($columns) > 0) {
            return sprintf(
                'SELECT %s FROM %s.%s',
                implode(', ', array_map(function ($column) {
                    return $this->quote($column);
                }, $columns)),
                $this->quote($table['schema']),
                $this->quote($table['tableName'])
            );
        } else {
            return sprintf(
                'SELECT * FROM %s.%s',
                $this->quote($table['schema']),
                $this->quote($table['tableName'])
            );
        }
    }

    private function quote(string $obj): string
    {
        $q = '"';
        return ($q . str_replace("$q", "$q$q", $obj) . $q);
    }
}
