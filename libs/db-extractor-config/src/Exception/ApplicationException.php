<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Exception;

use Exception;
use Keboola\CommonExceptions\ApplicationExceptionInterface;

class ApplicationException extends Exception implements ApplicationExceptionInterface
{

}
