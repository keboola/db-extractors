<?php

declare(strict_types=1);

use Keboola\DbExtractor\FunctionalTests\DatadirTest;
use Keboola\DbExtractor\FunctionalTests\DatabaseManager;

return function (DatadirTest $test): void {
    $manager = new DatabaseManager($test->getConnection());

    // sales table
    $manager->createSalesTable();
    $manager->generateSalesRows();
    $manager->addSalesConstraint('sales', ['createdat']);
    $test->getConnection()->exec('GRANT SELECT ON sales TO readonly_user;');

    // special table
    $manager->createEscapingTable();
    $manager->generateEscapingRows();
    $test->getConnection()->exec('GRANT SELECT ON escaping TO readonly_user;');
};
