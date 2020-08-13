<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\ValueObject;

class ExportResult
{
    protected string $csvPath;

    protected int $rowsCount;

    protected ?string $incFetchingColMaxValue;

    public function __construct(string $csvPath, int $rowsCount, ?string $incFetchingColMaxValue)
    {
        $this->csvPath = $csvPath;
        $this->rowsCount = $rowsCount;
        $this->incFetchingColMaxValue = $incFetchingColMaxValue;
    }

    public function getCsvPath(): string
    {
        return $this->csvPath;
    }

    public function getRowsCount(): int
    {
        return $this->rowsCount;
    }

    public function getIncFetchingColMaxValue(): ?string
    {
        return $this->incFetchingColMaxValue;
    }
}
