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
}
