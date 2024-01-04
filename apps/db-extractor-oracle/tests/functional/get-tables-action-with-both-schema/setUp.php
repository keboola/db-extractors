<?php

declare(strict_types=1);

use Keboola\DbExtractor\FunctionalTests\DatadirTest;
use Keboola\DbExtractor\FunctionalTests\TestConnection;

return function (DatadirTest $test): void {
    $connection = TestConnection::createConnection();

    $connection->exec('CREATE TABLE REGIONS AS SELECT * FROM HR.REGIONS');

    $connection->exec('ALTER TABLE REGIONS DROP COLUMN REGION_NAME');
};
