<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests;

use Keboola\DbExtractor\Exception\UserException;
use PDO;
use Throwable;

trait InsertRowsTrait
{
    use QuoteTrait;
    use QuoteIdentifierTrait;

    protected PDO $connection;

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
                        if ($value === 'GETDATE()') {
                            return $value;
                        }
                        return $this->quote((string) $value);
                    }, $row),
                ) .
                ')';
        }
        // In informix cannot be multiple values in one INSERT statement
        foreach ($valuesSql as $values) {
            try {
                $this->connection->query(sprintf(
                    'INSERT INTO %s.%s (%s) VALUES %s',
                    $this->quoteIdentifier((string) getenv('REDSHIFT_DB_SCHEMA')),
                    $this->quoteIdentifier($tableName),
                    implode(', ', $columnsSql),
                    $values,
                ));
            } catch (Throwable $e) {
                throw new UserException($e->getMessage(), $e->getCode(), $e);
            }
        }
    }
}
