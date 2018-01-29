# MySQL DB Extractor
[![Build Status](https://travis-ci.org/keboola/db-extractor-mysql.svg?branch=master)](https://travis-ci.org/keboola/db-extractor-mysql)

### Development

- clone the repository
- `docker-compose build`

#### Code Sniffer

- `docker-compose run --rm dev /code/vendor/bin/phpcs --standard=psr2 -n --ignore=vendor --extensions=php .` 

#### Static Analysis

- `docker-compose run --rm dev /code/vendor/bin/phpstan analyse --level=7 ./src ./tests`

#### Tests

- `docker-compose run --rm dev`

