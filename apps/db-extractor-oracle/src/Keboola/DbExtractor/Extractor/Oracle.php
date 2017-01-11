<?php
/**
 * @package ex-db-oracle
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\DbExtractor\Extractor;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Exception\ApplicationException;

class Oracle extends Extractor
{
	protected $db;

	public function createConnection($params)
	{
		$dbString = '//' . $params['host'] . ':' . $params['port'] . '/' . $params['database'];

		return oci_connect($params['user'], $params['password'], $dbString, 'AL32UTF8');
	}

	protected function executeQuery($query, CsvFile $csv)
	{
		$stmt = oci_parse($this->db, $query);
		oci_execute($stmt);

		$resultRow = oci_fetch_assoc($stmt);

		if (is_array($resultRow) && !empty($resultRow)) {
			// write header and first line
			$csv->writeRow(array_keys($resultRow));
			$csv->writeRow($resultRow);

			// write the rest
			while ($resultRow = oci_fetch_assoc($stmt)) {
				$csv->writeRow($resultRow);
			}
		} else {
			$this->logger->warn("Query returned empty result. Nothing was imported.");
		}
	}

	public function getConnection()
	{
		return $this->db;
	}

	public function testConnection()
	{
		$stmt = oci_parse($this->db, 'SELECT CURRENT_DATE FROM dual');
		oci_execute($stmt);
	}
}
