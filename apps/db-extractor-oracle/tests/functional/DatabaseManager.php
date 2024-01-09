<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\FunctionalTests;

use Keboola\DbExtractor\TraitTests\Tables\AutoIncrementTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\EscapingTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\SalesTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\TypesTableTrait;

class DatabaseManager
{
    use AutoIncrementTableTrait;
    use SalesTableTrait;
    use EscapingTableTrait;
    use TypesTableTrait;

    protected TestConnection $connection;

    public function __construct(TestConnection $connection)
    {
        $this->connection = $connection;
    }
}
