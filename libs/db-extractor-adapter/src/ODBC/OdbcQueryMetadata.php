<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\ODBC;

use Keboola\DbExtractor\Adapter\ValueObject\QueryMetadata;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\ColumnBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\ColumnCollection;

class OdbcQueryMetadata implements QueryMetadata
{
    /** @var resource */
    protected $stmt;

    protected ?ColumnCollection $columns = null;

    /**
     * @param resource $stmt
     */
    public function __construct($stmt)
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
        $columnsCount = odbc_num_fields($this->stmt);
        $columns = [];
        for ($i = 1; $i <= $columnsCount; $i++) {
            $builder = ColumnBuilder::create();
            $builder->setName((string) odbc_field_name($this->stmt, $i));
            $builder->setType((string) odbc_field_type($this->stmt, $i));
            $columns[] = $builder->build();
        }

        return new ColumnCollection($columns);
    }
}
