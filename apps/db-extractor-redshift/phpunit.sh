#!/usr/bin/env bash

# install dependencies
composer selfupdate;
composer install -n;

php ./tests/Keboola/loadS3.php

# run test suite
export ROOT_PATH="/code";
./vendor/bin/phpunit;
