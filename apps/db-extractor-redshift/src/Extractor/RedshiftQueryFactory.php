<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Generator;
use Keboola\DbExtractor\Adapter\Connection\DbConnection;
use Keboola\DbExtractor\Adapter\Query\DefaultQueryFactory;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;

class RedshiftQueryFactory extends DefaultQueryFactory
{
    private ?string $incrementalFetchingColType = null;

    public function setIncrementalFetchingColType(string $incrementalFetchingColType): self
    {
        $this->incrementalFetchingColType = $incrementalFetchingColType;
        return $this;
    }

    protected function createWhere(ExportConfig $exportConfig, DbConnection $connection): Generator
    {
        if ($exportConfig->isIncrementalFetching() && isset($this->state['lastFetchedRow'])) {
            if ($this->incrementalFetchingColType === Redshift::INCREMENT_TYPE_NUMERIC) {
                $lastFetchedRow = $this->state['lastFetchedRow'];
            } else {
                $lastFetchedRow = $connection->quote((string) $this->state['lastFetchedRow']);
            }
            yield sprintf(
            // intentionally ">=" last row should be included, it is handled by storage deduplication process
                'WHERE %s >= %s',
                $connection->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
                $lastFetchedRow
            );
        }
    }
}
