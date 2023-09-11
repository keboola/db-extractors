<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Generator;
use Keboola\DbExtractor\Adapter\Connection\DbConnection;
use Keboola\DbExtractor\Adapter\Query\DefaultQueryFactory;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use function Keboola\Utils\formatDateTime;

class OracleQueryFactory extends DefaultQueryFactory
{
    private ?string $incrementalFetchingColType = null;

    public function __construct(array $state)
    {
        parent::__construct($state);
    }

    public function create(ExportConfig $exportConfig, DbConnection $connection): string
    {
        $sql = array_merge(
            iterator_to_array($this->createSelect($exportConfig, $connection)),
            iterator_to_array($this->createFrom($exportConfig, $connection)),
            iterator_to_array($this->createWhere($exportConfig, $connection)),
            iterator_to_array($this->createOrderBy($exportConfig, $connection))
        );
        return implode(' ', $sql);
    }

    public function createLastRowQuery(ExportConfig $exportConfig, DbConnection $connection): string
    {
        return sprintf(
            'SELECT %s FROM (SELECT * FROM (%s) ORDER BY %s DESC) WHERE ROWNUM = 1',
            $connection->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
            $this->create($exportConfig, $connection),
            $connection->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
        );
    }

    public function setIncrementalFetchingColType(?string $incrementalFetchingColType): self
    {
        $this->incrementalFetchingColType = $incrementalFetchingColType;
        return $this;
    }

    protected function createWhere(ExportConfig $exportConfig, DbConnection $connection): Generator
    {
        $where = [];
        if ($exportConfig->isIncrementalFetching() && isset($this->state['lastFetchedRow'])) {
            switch ($this->incrementalFetchingColType) {
                case Oracle::INCREMENT_TYPE_NUMERIC:
                    $lastFetchedRow = $this->state['lastFetchedRow'];
                    break;
                case Oracle::INCREMENT_TYPE_DATE:
                    $lastFetchedRow = sprintf(
                        "DATE '%s'",
                        formatDateTime($this->state['lastFetchedRow'], 'Y-m-d')
                    );
                    break;
                case Oracle::INCREMENT_TYPE_TIMESTAMP:
                    $lastFetchedRow = sprintf(
                        "TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF')",
                        $this->state['lastFetchedRow']
                    );
                    break;
                default:
                    $lastFetchedRow = $connection->quote((string) $this->state['lastFetchedRow']);
            }

            // intentionally ">=" last row should be included, it is handled by storage deduplication process
            $where[] = sprintf(
                '%s >= %s',
                $connection->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
                $lastFetchedRow
            );
        }

        if ($exportConfig->hasIncrementalFetchingLimit()) {
            $where[] = sprintf('ROWNUM <= %d', $exportConfig->getIncrementalFetchingLimit());
        }

        if ($where) {
            yield sprintf('WHERE %s', implode(' AND ', $where));
        }
    }

    public function createCheckNullsQuery(ExportConfig $exportConfig, OracleDbConnection $connection): string
    {
        $from = iterator_to_array($this->createFrom($exportConfig, $connection));
        return sprintf(
            'SELECT COUNT(*) %s WHERE %s IS NULL',
            reset($from),
            $connection->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
        );
    }
}
