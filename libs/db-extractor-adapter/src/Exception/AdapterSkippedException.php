<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\Exception;

use Keboola\CommonExceptions\UserExceptionInterface;

/**
 * The adapter cannot be used due to any condition.
 */
interface AdapterSkippedException extends UserExceptionInterface
{

}
