<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\ValueObject;

class ExportResult
{
    protected string $csvPath;

    protected int $rowsCount;

    protected QueryMetadata $queryMetadata;

    protected ?string $incFetchingColMaxValue;

    public function __construct(
        string $csvPath,
        int $rowsCount,
        QueryMetadata $queryMetadata,
        ?string $incFetchingColMaxValue
    ) {
        $this->csvPath = $csvPath;
        $this->rowsCount = $rowsCount;
        $this->queryMetadata = $queryMetadata;
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

    public function getIncFetchingColMaxValue(): ?string
    {
        return $this->incFetchingColMaxValue;
    }
}
