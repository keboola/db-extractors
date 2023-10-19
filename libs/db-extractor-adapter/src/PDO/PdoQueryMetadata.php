<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\PDO;

use Keboola\DbExtractor\Adapter\Exception\InvalidStateException;
use Keboola\DbExtractor\Adapter\ValueObject\QueryMetadata;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\ColumnBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\ColumnCollection;
use PDOStatement;

class PdoQueryMetadata implements QueryMetadata
{
    protected PDOStatement $stmt;

    protected ?ColumnCollection $columns = null;

    public function __construct(PDOStatement $stmt)
    {
        $this->stmt = $stmt;
    }

    public function getColumns(): ColumnCollection
    {
        if (!$this->columns) {
            $this->columns = $this->doGetColumns();
        }

        return $this->columns;
    }

    private function doGetColumns(): ColumnCollection
    {
        $columnsCount = $this->stmt->columnCount();
        $columns = [];
        for ($i = 0; $i < $columnsCount; $i++) {
            /** @var array $columnMetadata */
            $columnMetadata = $this->stmt->getColumnMeta($i);

            // Method getColumnMeta may not be supported by a PDO driver
            $missingKeys = array_diff(['name'], array_keys($columnMetadata));
            if ($missingKeys) {
                throw new InvalidStateException(sprintf(
                    'Missing key "%s" in PDO query column\'s metadata.',
                    implode('", "', $missingKeys),
                ));
            }

            $builder = ColumnBuilder::create();
            $builder->setName($columnMetadata['name']);
            $builder->setType($columnMetadata['native_type'] ?? 'string');
            $columns[] = $builder->build();
        }

        return new ColumnCollection($columns);
    }
}
