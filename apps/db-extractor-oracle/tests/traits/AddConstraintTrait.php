<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests;

use Keboola\DbExtractor\FunctionalTests\TestConnection;

trait AddConstraintTrait
{
    use QuoteIdentifierTrait;

    protected TestConnection $connection;

    public function addConstraint(
        string $tableName,
        string $name,
        string $type,
        string $value,
        ?string $reference = null
    ): void {
        $sql = sprintf(
            'ALTER TABLE %s ADD CONSTRAINT %s %s (%s)',
            $this->quoteIdentifier($tableName),
            $name,
            $type,
            $value
        );

        if ($reference) {
            $sql = sprintf('%s REFERENCES %s', $sql, $reference);
        }

        $this->connection->exec($sql);
    }
}
