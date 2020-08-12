<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\Tests\Fixtures;

use RuntimeException;
use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;

class FailingExportAdapterException extends RuntimeException
{
}
