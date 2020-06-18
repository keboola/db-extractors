<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject;

use Countable;
use Iterator;
use IteratorAggregate;
use ArrayIterator;
use Keboola\DbExtractor\TableResultFormat\Exception\TableNotFoundException;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject;

/**
 * @implements IteratorAggregate<Table>
 */
class TableCollection implements ValueObject, Countable, IteratorAggregate
{
    /** @var Table[] */
    private array $tables;

    /**
     * @internal Should be created using MetadataBuilder, don't call it directly!
     * @param Table[] $tables
     */
    public function __construct(array $tables)
    {
        $this->tables =  $tables;
    }

    public function count(): int
    {
        return count($this->tables);
    }

    /**
     * @return Iterator<Table>
     */
    public function getIterator(): Iterator
    {
        return new ArrayIterator($this->tables);
    }

    /**
     * @return Table[]
     */
    public function getAll(): array
    {
        return $this->tables;
    }

    public function isEmpty(): bool
    {
        return empty($this->tables);
    }

    public function getByNameAndSchema(string $name, string $schema, bool $caseSensitive = false): Table
    {
        try {
            // First, let's try to find an exact match
            return $this->doGetByNameAndSchema($name, $schema, true);
        } catch (TableNotFoundException $e) {
            if ($caseSensitive === true) {
                throw $e;
            }

            // Try to find by case-insensitive
            return $this->doGetByNameAndSchema($name, $schema, false);
        }
    }

    protected function doGetByNameAndSchema(string $name, string $schema, bool $caseSensitive): Table
    {
        foreach ($this->tables as $table) {
            if (($table->getName() === $name && $table->getSchema() === $schema) ||
                (
                    !$caseSensitive &&
                    mb_strtolower($table->getName()) === mb_strtolower($name) &&
                    mb_strtolower($table->getSchema()) === mb_strtolower($schema)
                )
            ) {
                return $table;
            }
        }

        throw new TableNotFoundException(sprintf(
            'Table with name "%s" and schema "%s" not found.',
            $name,
            $schema
        ));
    }
}
