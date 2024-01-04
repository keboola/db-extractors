<?php

declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

if (!getenv('REDSHIFT_DB_SCHEMA')) {
    putenv('REDSHIFT_DB_SCHEMA=public');
}
