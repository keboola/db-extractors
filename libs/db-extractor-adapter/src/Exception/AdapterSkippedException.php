<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\Exception;

use Keboola\CommonExceptions\UserExceptionInterface;

/**
 * The adapter cannot be used due to any condition (see FallbackExportAdapter).
 */
interface AdapterSkippedException extends UserExceptionInterface
{

}
