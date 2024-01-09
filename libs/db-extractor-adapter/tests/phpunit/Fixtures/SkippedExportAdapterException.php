<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\Tests\Fixtures;

use Keboola\DbExtractor\Adapter\Exception\AdapterSkippedException;
use RuntimeException;

class SkippedExportAdapterException extends RuntimeException implements AdapterSkippedException
{
}
