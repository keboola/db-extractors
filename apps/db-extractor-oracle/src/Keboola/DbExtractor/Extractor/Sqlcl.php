<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Logger;
use Keboola\Temp\Temp;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Process\Process;
use SplFileInfo;

class Sqlcl
{
    private const SQLCL_SH = 'sqlclsh';

    /** @var TEMP */
    private $tmp;

    /** @var array */
    private $dbParams;

    /** @var Logger */
    private $logger;

    public function __construct(array $dbParams, Logger $logger)
    {
        $this->dbParams = $dbParams;
        $this->logger = $logger;
        $this->tmp = new Temp();
    }

    public function export(string $query, string $filename): int
    {
        $runfile = $this->createSqlclCommandFile($filename, $query);

        $process = new Process('/bin/bash ' . $runfile->getPathname());
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

    private function createSqlclCommandFile(string $filename, string $query): SplFileInfo
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

        $runfile = $this->tmp->createFile(self::SQLCL_SH);

        $fd = $runfile->openFile('w');

        $fd->fwrite($fullcmd);

        return $runfile;
    }
}
