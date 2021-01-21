<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests;

use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\FunctionalTests\TestConnection;
use Throwable;

trait InsertRowsTrait
{
    use QuoteTrait;
    use QuoteIdentifierTrait;

    protected TestConnection $connection;

    public function insertRows(string $tableName, array $columns, array $rows): void
    {
        // Generate columns statement
        $columnsSql = [];
        foreach ($columns as $name) {
            $columnsSql[] = $this->quoteIdentifier($name);
        }

        // Generate values statement
        $valuesSql = [];
        foreach ($rows as $row) {
            $valuesSql[] =
                '(' .
                implode(
                    ', ',
                    array_map(function ($value) {
                        if ($value === null) {
                            return 'NULL';
                        }
                        if (substr((string) $value, 0, 7) === 'TO_DATE') {
                            return $value;
                        }
                        return $this->quote((string) $value);
                    }, $row)
                ) .
                ')';
        }
        // In informix cannot be multiple values in one INSERT statement
        foreach ($valuesSql as $values) {
            try {
                $sql = sprintf(
                    'INSERT INTO %s (%s) VALUES %s',
                    $this->quoteIdentifier($tableName),
                    implode(', ', $columnsSql),
                    $values
                );

                $this->connection->exec($sql);
            } catch (Throwable $e) {
                throw new UserException($e->getMessage(), $e->getCode(), $e);
            }
        }
    }
}
