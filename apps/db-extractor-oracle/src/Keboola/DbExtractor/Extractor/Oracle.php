<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Logger;
use Keboola\DbExtractor\RetryProxy;
use Keboola\Utils;
use Symfony\Component\Process\Process;

use Throwable;

class Oracle extends Extractor
{
    protected $db;

    /** @var  array */
    protected $dbParams;

    /** @var  array */
    protected $exportConfigFiles;

    public function __construct(array $parameters, array $state = [], ?Logger $logger = null)
    {
        $this->dbParams = $parameters['db'];
        parent::__construct($parameters, $state, $logger);
        // setup the export config files for the export tool
        foreach ($parameters['tables'] as $table) {
            $this->exportConfigFiles[$table['name']] = $this->dataDir . "/" . $table['id'] . ".json";
            $this->writeExportConfig($this->dbParams, $table);
        }
    }

    private function writeExportConfig(array $dbParams, array $table): void
    {
        if (!isset($table['query'])) {
            $table['query'] = $this->simpleQuery($table['table'], $table['columns']);
        } else {
            $table['query'] = rtrim($table['query'], " ;");
        }
        $table['outputFile'] = $this->getOutputFilename($table['outputTable']);
        $dbParams['port'] = (string) $dbParams['port'];
        $parameters = array(
            'db' => $dbParams
        );
        $config = array(
            'parameters' => array_merge($parameters, $table)
        );
        file_put_contents($this->exportConfigFiles[$table['name']], json_encode($config));
    }

    public function createConnection(array $params)
    {
        $dbString = '//' . $params['host'] . ':' . $params['port'] . '/' . $params['database'];
        $connection = oci_connect($params['user'], $params['password'], $dbString, 'AL32UTF8');

        if (!$connection) {
            $error = oci_error();
            throw new UserException("Error connection to DB: " . $error['message']);
        }
        return $connection;
    }

    public function createSshTunnel(array $dbConfig): array
    {
        $this->dbParams = parent::createSshTunnel($dbConfig);
        return $this->dbParams;
    }

    protected function handleDbError(Throwable $e, ?array $table = null, ?int $counter = null): UserException
    {
        $message = "";
        if ($table) {
            $message = sprintf("[%s]: ", $table['name']);
        }
        $message .= sprintf('DB query failed: %s', $e->getMessage());
        if ($counter) {
            $message .= sprintf(' Tried %d times.', $counter);
        }
        return new UserException($message, 0, $e);
    }

    public function export(array $table): array
    {
        $outputTable = $table['outputTable'];
        $csv = $this->createOutputCsv($outputTable);

        $this->logger->info("Exporting to " . $outputTable);

        $isAdvancedQuery = true;
        if (array_key_exists('table', $table) && !array_key_exists('query', $table)) {
            $isAdvancedQuery = false;
            $query = $this->simpleQuery($table['table'], $table['columns']);
        } else {
            $query = $table['query'];
        }
        $maxTries = isset($table['retries']) ? (int) $table['retries'] : null;

        $proxy = new RetryProxy($this->logger, $maxTries);
        $tableName = $table['name'];
        try {
            $linesWritten = $proxy->call(function () use ($tableName, $isAdvancedQuery) {
                try {
                    return $this->exportTable($tableName, $isAdvancedQuery);
                } catch (Throwable $e) {
                    try {
                        $this->db = $this->createConnection($this->dbParams);
                    } catch (Throwable $e) {
                    };
                    throw $e;
                }
            });
        } catch (Throwable $e) {
            throw $this->handleDbError($e, $table, $maxTries);
        }
        $rowCount = $linesWritten - 1;
        if ($rowCount > 0) {
            $this->createManifest($table);
        } else {
            @unlink($csv->getPathname());
            $this->logger->warn(
                sprintf(
                    "Query returned empty result. Nothing was imported for table [%s]",
                    $table['name']
                )
            );
        }

        $output = [
            "outputTable"=> $outputTable,
            "rows" => $rowCount,
        ];
        return $output;
    }

    protected function exportTable(string $tableName, bool $advancedQuery): int
    {
        $cmd = 'java -jar /code/oracle/table-exporter.jar ' . $this->exportConfigFiles[$tableName];
        $cmd .= ($advancedQuery) ? ' true' : ' false';

        $process = new Process(
            $cmd
        );
        $process->setTimeout(null);
        $process->setIdleTimeout(null);
        $process->run();

        if (!$process->isSuccessful()) {
            throw new \ErrorException('Export process failed: ' . $process->getErrorOutput());
        }
        // log the process output
        $output = $process->getOutput();
        $this->logger->info($output);

        $fetchedPos = strpos($output, "Fetched");
        $rowCountStr = substr($output, $fetchedPos, strpos($output, "rows in") - $fetchedPos);
        $linesWritten = (int) filter_var(
            $rowCountStr,
            FILTER_SANITIZE_NUMBER_INT
        );
        return $linesWritten;
    }

    public function testConnection(): bool
    {
        $stmt = oci_parse($this->db, 'SELECT CURRENT_DATE FROM dual');
        $success = oci_execute($stmt);
        oci_free_statement($stmt);
        return $success;
    }

    public function getTables(array $tables = null): array
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

        // reset the connection because after a long export it may have been dropped
        if (!is_null($tables)) {
            @oci_close($this->db);
            $this->db = $this->createConnection($this->dbParams);
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
                    "sanitizedName" => Utils\sanitizeColumnName($column["COLUMN_NAME"]),
                    "type" => $column['DATA_TYPE'],
                    "nullable" => ($column['NULLABLE'] === 'Y') ? true : false,
                    "length" => $length,
                    "ordinalPosition" => $column['COLUMN_ID'],
                    "primaryKey" => false,
                    "uniqueKey" => false,
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
            ksort($tableDefs[$curTable]['columns']);
        }
        ksort($tableDefs);
        return array_values($tableDefs);
    }

    public function simpleQuery(array $table, array $columns = array()): string
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
