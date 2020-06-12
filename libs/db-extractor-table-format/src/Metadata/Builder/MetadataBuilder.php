<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TableResultFormat\Metadata\Builder;

use Generator;
use Keboola\DbExtractor\TableResultFormat\Exception\NoColumnException;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\TableCollection;

class MetadataBuilder implements Builder
{
    /** @var string[] */
    private array $tableRequiredProperties;

    /** @var string[] */
    private array $columnRequiredProperties;

    /** @var TableBuilder[] */
    private array $tables = [];

    /**
     * In some databases (eg. MySql),
     * the user may have the permission to list tables,
     * but without permission to list the columns in the table.
     * Such table cannot be used even for export and therefore we ignore it.
     */
    private bool $ignoreTableWithoutColumns = true;

    public static function create(array $tableRequiredProperties = [], array $columnRequiredProperties = []): self
    {
        return new self($tableRequiredProperties, $columnRequiredProperties);
    }

    protected function __construct(array $tableRequiredProperties = [], array $columnRequiredProperties = [])
    {
        $this->tableRequiredProperties = $tableRequiredProperties;
        $this->columnRequiredProperties = $columnRequiredProperties;
    }

    public function setIgnoreTableWithoutColumns(bool $value): self
    {
        $this->ignoreTableWithoutColumns = $value;
        return $this;
    }

    public function build(): TableCollection
    {
        return new TableCollection(iterator_to_array($this->buildTables()));
    }

    public function addTable(): TableBuilder
    {
        $table = TableBuilder::create($this->tableRequiredProperties, $this->columnRequiredProperties);
        $this->tables[] = $table;
        return $table;
    }

    private function buildTables(): Generator
    {
        foreach ($this->tables as $table) {
            try {
                yield $table->build();
            } catch (NoColumnException $e) {
                if ($this->ignoreTableWithoutColumns === false) {
                    throw $e;
                }
            }
        }
    }
}
