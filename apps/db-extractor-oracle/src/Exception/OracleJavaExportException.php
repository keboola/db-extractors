<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Exception;

use Keboola\CommonExceptions\ApplicationExceptionInterface;
use RuntimeException;

class OracleJavaExportException extends RuntimeException implements ApplicationExceptionInterface
{

}
