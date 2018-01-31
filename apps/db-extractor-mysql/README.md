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

#### Configurations

The configuration is separated into 2 parts: 
1. db -- where the connection is defined, 
2. tables -- where the extractions are defined.
  
There are 2 possible types of table extraction.  
1. A table defined by `schema` and `tableName`, this option can also include a columns list.
2. A `query` which is the SQL SELECT statement to be executed to produce the result table.

```yaml
parameters:
    db:
        host: localhost
        database: test
        user: test
        password:
        port: 3306

    tables:
        -
            id: 0
            name: escaping
            query: "SELECT * FROM escaping"
            outputTable: in.c-main.escaping
            incremental: false
            primaryKey:
              -
                col1
            enabled: true
        -
            id: 1
            name: simple
            outputTable: in.c-main.simple
            incremental: false
            primaryKey: null
            table:
              schema: testdb
              tableName: escaping
            retries: 1
            columns:
```