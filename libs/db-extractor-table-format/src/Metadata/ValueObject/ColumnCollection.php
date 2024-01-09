<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject;

use ArrayIterator;
use Countable;
use Iterator;
use IteratorAggregate;
use Keboola\DbExtractor\TableResultFormat\Exception\ColumnNotFoundException;
use Keboola\DbExtractor\TableResultFormat\Exception\InvalidArgumentException;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject;

/**
 * @implements IteratorAggregate<Column>
 */
class ColumnCollection implements ValueObject, Countable, IteratorAggregate
{
    /** @var Column[] */
    private array $columns;

    /**
     * @internal Should be created using TableBuilder, don't call it directly!
     * @param Column[] $columns
     */
    public function __construct(array $columns)
    {
        // Check columns type
        $allHaveOrdinalPosSet = null;
        array_walk($columns, function ($column) use (&$allHaveOrdinalPosSet): void {
            if (!$column instanceof Column) {
                throw new InvalidArgumentException(sprintf(
                    'All columns must by of type Column, given "%s".',
                    is_object($column) ? get_class($column) : gettype($column),
                ));
            }

            if ($allHaveOrdinalPosSet !== null && $allHaveOrdinalPosSet !== $column->hasOrdinalPosition()) {
                throw new InvalidArgumentException(sprintf(
                    'Set "ordinalPosition" to all columns, or none. ' .
                    'Column "%s" has hasOrdinalPosition = "%s", but the previous value is "%s".',
                    $column->getName(),
                    $column->hasOrdinalPosition() ? 'true' : 'false',
                    $allHaveOrdinalPosSet ? 'true' : 'false',
                ));
            }

            $allHaveOrdinalPosSet = $column->hasOrdinalPosition();
        });

        // Sort by ordinalPosition if present
        if ($allHaveOrdinalPosSet) {
            usort($columns, function (Column $a, Column $b): int {
                return $a->getOrdinalPosition() <=> $b->getOrdinalPosition();
            });
        }

        $this->columns =  $columns;
    }

    public function count(): int
    {
        return count($this->columns);
    }

    /**
     * @return Iterator<Column>
     */
    public function getIterator(): Iterator
    {
        return new ArrayIterator($this->columns);
    }

    /**
     * @return Column[]
     */
    public function getAll(): array
    {
        return $this->columns;
    }

    /**
     * @return string[]
     */
    public function getNames(): array
    {
        return array_map(fn(Column $col) => $col->getName(), $this->columns);
    }

    public function isEmpty(): bool
    {
        return empty($this->columns);
    }

    public function getByName(string $name, bool $caseSensitive = false): Column
    {
        $column = $this->searchByString('getName', $name, $caseSensitive);
        if ($column) {
            return $column;
        }

        throw new ColumnNotFoundException(sprintf('Column with name "%s" not found.', $name));
    }

    public function getBySanitizedName(string $sanitizedName, bool $caseSensitive = false): Column
    {
        $column = $this->searchByString('getSanitizedName', $sanitizedName, $caseSensitive);
        if ($column) {
            return $column;
        }

        throw new ColumnNotFoundException(sprintf('Column with sanitized "%s" not found.'. $sanitizedName));
    }

    public function getByOrdinalPosition(int $ordinalPosition): Column
    {
        foreach ($this->columns as $column) {
            if ($column->getOrdinalPosition() === $ordinalPosition) {
                return $column;
            }
        }

        throw new ColumnNotFoundException(sprintf('Column with ordinal position "%d" not found.', $ordinalPosition));
    }

    protected function searchByString(string $method, string $value, bool $caseSensitive): ?Column
    {
        // First, let's try to find an exact match
        foreach ($this->columns as $column) {
            if ($column->$method() === $value) {
                return $column;
            }
        }

        // Try to find by case-insensitive
        if ($caseSensitive === false) {
            foreach ($this->columns as $column) {
                if (mb_strtolower($column->$method()) === mb_strtolower($value)) {
                    return $column;
                }
            }
        }

        return null;
    }
}
