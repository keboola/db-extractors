<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Logger;
use Keboola\Temp\Temp;
use Symfony\Component\Process\Process;
use SplFileInfo;

class Sqlcl
{
    private const SQLCL_SH = 'sqlclsh';

    /** @var Temp */
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
        /** @var Process $process */
        $process = $this->runSqlclProcess($filename, $query);

        if (!$process->isSuccessful()) {
            // there was an error.  We'll run it again with feedback on to retrieve the error.
            $errorFile = $this->tmp->createTmpFile('errorFile');
            $errorProcess = $this->runSqlclProcess($errorFile->getPathname(), $query, "ON");
            throw new UserException(sprintf(
                "Export process failed.\n\n Error Output: %s.",
                file_get_contents($errorFile->getPathname())
            ));
        }

        $process = new Process(sprintf("cat %s | wc -l", $filename));
        $process->run();
        if (!$process->isSuccessful()) {
            throw new ApplicationException("Was unable to get the number of lines from the output file.");
        }
        $this->logger->info("SQLCL export completed successfully.");

        $lineCount = (int) $process->getOutput();

        return ($lineCount <= 1) ? 0 : $lineCount;
    }

    private function runSqlclProcess(string $filename, string $query, string $feedback = "OFF")
    {
        $runfile = $this->createSqlclCommandFile($filename, $query, $feedback);

        $process = new Process('/bin/bash ' . $runfile->getPathname());
        $process->setTimeout(null);
        $process->run();

        return $process;
    }

    private function createSqlclCommandFile(string $filename, string $query, string $feedback): SplFileInfo
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
SET SQLFORMAT CSV\n
SET FEEDBACK %s\n
SET ENCODING UTF-8\n
SET ERRORLOGGING OFF\n
whenever sqlerror exit sql.sqlcode\n
SPOOL %s\n
%s;\n
SPOOL OFF\n
EOT;

        $cmd = sprintf(
            $cmdString,
            $feedback,
            $filename,
            rtrim($query, ';')
        );

        $fullcmd = sprintf(
            "#!/bin/bash\nexport SQLFORMAT=CSV\nexport FEEDBACK=%s\necho -e \"%s\" | ./oracle/sqlcl/bin/sql %s 1>/dev/null",
            $feedback,
            $cmd,
            $connectionString
        );

        $this->logger->info(sprintf(
            "Executing this SQLCL command: %s",
            $cmd
        ));

        $runfile = $this->tmp->createFile(self::SQLCL_SH);

        $fd = $runfile->openFile('w');

        $fd->fwrite($fullcmd);

        return $runfile;
    }
}
