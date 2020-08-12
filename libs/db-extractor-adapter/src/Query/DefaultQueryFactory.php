<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\Query;

use Keboola\DbExtractor\Adapter\Connection\DbConnection;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;

class DefaultQueryFactory implements QueryFactory
{
    protected array $state;

    public function __construct(array $state)
    {
        $this->state = $state;
    }

    public function create(ExportConfig $exportConfig, DbConnection $connection): string
    {
        $sql = [];

        if ($exportConfig->hasColumns()) {
            $sql[] = sprintf('SELECT %s', implode(', ', array_map(
                fn(string $c) => $connection->quoteIdentifier($c),
                $exportConfig->getColumns()
            )));
        } else {
            $sql[] = 'SELECT *';
        }

        $sql[] = sprintf(
            'FROM %s.%s',
            $connection->quoteIdentifier($exportConfig->getTable()->getSchema()),
            $connection->quoteIdentifier($exportConfig->getTable()->getName())
        );

        if ($exportConfig->isIncrementalFetching() && isset($this->state['lastFetchedRow'])) {
            $sql[] = sprintf(
                // intentionally ">=" last row should be included, it is handled by storage deduplication process
                'WHERE %s >= %s',
                $connection->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
                $connection->quote($this->state['lastFetchedRow'])
            );
        }

        if ($exportConfig->hasIncrementalFetchingLimit()) {
            $sql[] = sprintf(
                'ORDER BY %s LIMIT %d',
                $connection->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
                $exportConfig->getIncrementalFetchingLimit()
            );
        }

        return implode(' ', $sql);
    }
}
