<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\Query;

use Generator;
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
        $sql = array_merge(
            iterator_to_array($this->createSelect($exportConfig, $connection)),
            iterator_to_array($this->createFrom($exportConfig, $connection)),
            iterator_to_array($this->createWhere($exportConfig, $connection)),
            iterator_to_array($this->createOrderBy($exportConfig, $connection)),
            iterator_to_array($this->createLimit($exportConfig, $connection)),
        );
        return implode(' ', $sql);
    }

    protected function createSelect(ExportConfig $exportConfig, DbConnection $connection): Generator
    {
        if ($exportConfig->hasColumns()) {
            yield sprintf('SELECT %s', implode(', ', array_map(
                fn(string $c) => $connection->quoteIdentifier($c),
                $exportConfig->getColumns(),
            )));
        } else {
            yield 'SELECT *';
        }
    }

    protected function createFrom(ExportConfig $exportConfig, DbConnection $connection): Generator
    {
        yield sprintf(
            'FROM %s.%s',
            $connection->quoteIdentifier($exportConfig->getTable()->getSchema()),
            $connection->quoteIdentifier($exportConfig->getTable()->getName()),
        );
    }

    protected function createWhere(ExportConfig $exportConfig, DbConnection $connection): Generator
    {
        if ($exportConfig->isIncrementalFetching() && isset($this->state['lastFetchedRow'])) {
            yield sprintf(
                // intentionally ">=" last row should be included, it is handled by storage deduplication process
                'WHERE %s >= %s',
                $connection->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
                $connection->quote($this->state['lastFetchedRow']),
            );
        }
    }

    protected function createOrderBy(ExportConfig $exportConfig, DbConnection $connection): Generator
    {
        if ($exportConfig->isIncrementalFetching()) {
            yield sprintf(
                'ORDER BY %s',
                $connection->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
            );
        }
    }

    protected function createLimit(ExportConfig $exportConfig, DbConnection $connection): Generator
    {
        if ($exportConfig->hasIncrementalFetchingLimit()) {
            yield sprintf(
                'LIMIT %d',
                $exportConfig->getIncrementalFetchingLimit(),
            );
        }
    }
}
