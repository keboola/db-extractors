<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\Tests\Fixtures;

use RuntimeException;
use Keboola\DbExtractor\Adapter\ExportAdapter;
use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;

class FailingExportAdapter implements ExportAdapter
{
    private string $name;

    private int $exportCallCount = 0;

    public function __construct(string $name)
    {
        $this->name = $name;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function export(ExportConfig $exportConfig, string $csvFileName): ExportResult
    {
        $this->exportCallCount++;
        throw new FailingExportAdapterException('Something went wrong.');
    }

    public function getExportCallCount(): int
    {
        return $this->exportCallCount;
    }
}
