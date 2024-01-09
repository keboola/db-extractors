<?php

declare(strict_types=1);

use Keboola\DbExtractor\FunctionalTests\DatadirTest;

return function (DatadirTest $test): void {
    $test->setKbcRealUser('my-real-user');
};
