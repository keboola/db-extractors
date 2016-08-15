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

        return new \PDO(
                "pgsql:dbname={$dbParams['database']};port={$port};host=" . $dbParams['host'],
                $dbParams['user'],
                $dbParams['password']
            );
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
        $query = $table['query'];
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
        
        ob_implicit_flush(true);
        while (@ob_end_flush());

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
}
