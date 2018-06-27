<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Logger;
use Symfony\Component\Process\Process;

class Sqlcl
{
    /** @var array */
    private $dbParams;

    /** @var Logger */
    private $logger;

    public function __construct(array $dbParams, Logger $logger)
    {
        $this->dbParams = $dbParams;
        $this->logger = $logger;
    }

    public function export(string $query, string $filename): int
    {
        $process = new Process($this->createSqlclCommand($filename, $query));
        $process->setTimeout(null);
        $process->run();

        if (!$process->isSuccessful()) {
            throw new UserException(sprintf(
                "Export process failed. Output: %s. \n\n Error Output: %s.",
                $process->getOutput(),
                $process->getErrorOutput()
            ));
        }

        $outputFile = new CsvFile($filename);
        $numRows = 0;
        $colCount = $outputFile->getColumnsCount();
        while ($outputFile->valid()) {
            if (count($outputFile->current()) !== $colCount) {
                throw new UserException("The SQLCL command produced an invalid csv.");
            }
            $outputFile->next();
            $numRows++;
        }
        $this->logger->info(sprintf("SQLCL successfully exported %d rows.", $numRows));
        return $numRows;
    }

    private function createSqlclCommand(string $filename, string $query): string
    {
        $connectionString = sprintf(
            "%s/%s@%s:%d/%s",
            $this->dbParams['user'],
            $this->dbParams['#password'],
            $this->dbParams['host'],
            $this->dbParams['port'],
            $this->dbParams['database']
        );

        $cmdString = <<<EOT
SET SQLFORMAT CSV\nSET FEEDBACK OFF\nSPOOL %s\n%s;\nSPOOL OFF\n
EOT;

        $cmd = sprintf(
            $cmdString,
            $filename,
            rtrim($query, ';')
        );

        $fullcmd = sprintf(
            "#!/bin/bash\nexport SQLFORMAT=CSV\nexport FEEDBACK=OFF\necho -e \"%s\" | ./oracle/sqlcl/bin/sql %s",
            $cmd,
            $connectionString
        );

        $this->logger->info(sprintf(
            "Executing this SQLCL command: %s", $fullcmd
        ));

        return $fullcmd;
    }
}
