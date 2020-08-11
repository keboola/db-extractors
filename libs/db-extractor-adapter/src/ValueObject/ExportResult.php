<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\ValueObject;

class ExportResult
{
    protected int $rowsCount;

    protected ?string $incFetchingColMaxValue;

    public function __construct(int $rowsCount, ?string $incFetchingColMaxValue)
    {
        $this->rowsCount = $rowsCount;
        $this->incFetchingColMaxValue = $incFetchingColMaxValue;
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
