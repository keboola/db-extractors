<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\ValueObject;

class ExportResult
{
    protected string $csvPath;

    protected int $rowsCount;

    protected QueryMetadata $queryMetadata;

    protected bool $csvHeaderPresent;

    protected ?string $incFetchingColMaxValue;

    public function __construct(
        string $csvPath,
        int $rowsCount,
        QueryMetadata $queryMetadata,
        bool $csvHeaderPresent,
        ?string $incFetchingColMaxValue,
    ) {
        $this->csvPath = $csvPath;
        $this->rowsCount = $rowsCount;
        $this->queryMetadata = $queryMetadata;
        $this->csvHeaderPresent = $csvHeaderPresent;
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

    public function getQueryMetadata(): QueryMetadata
    {
        return $this->queryMetadata;
    }

    public function hasCsvHeader(): bool
    {
        return $this->csvHeaderPresent;
    }

    public function getIncFetchingColMaxValue(): ?string
    {
        return $this->incFetchingColMaxValue;
    }
}
