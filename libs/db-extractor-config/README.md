# Database extractor config
Config definition and validation for database extractor

## Usage
Require with composer:

```yml

    composer require keboola/db-extractor-config

```

## Development

Clone this repository and init the workspace with following command:
```
    git clone git@github.com:keboola/db-extractor-config.git
    cd db-extractor-config
    docker-compose build
    docker-compose run --rm dev composer install --no-scripts
```

Run the test suite using this command:
```
    docker-compose run --rm dev composer tests
```

## License

MIT licensed, see [LICENSE](./LICENSE) file.
