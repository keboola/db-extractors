<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\FunctionalTests;

use Keboola\DbExtractor\TraitTests\Tables\AutoIncrementTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\EscapingTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\SalesTableTrait;
use \PDO;

class DatabaseManager
{
    use AutoIncrementTableTrait;
    use SalesTableTrait;
    use EscapingTableTrait;

    protected PDO $connection;

    public function __construct(PDO $connection)
    {
        $this->connection = $connection;
    }
}
