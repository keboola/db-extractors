# Database extractor SSH tunnel
Simple PHP class for create ssh tunnel

## Usage
Require with composer:

```yml

    composer require keboola/db-extractor-ssh-tunnel

```

## Development

Clone this repository and init the workspace with following command:
```
    git clone git@github.com:keboola/db-extractor-ssh-tunnel.git
    cd db-extractor-ssh-tunnel
    docker-compose build
    docker-compose run --rm dev composer install --no-scripts
```

Run the test suite using this command:
```
    docker-compose run --rm dev composer tests
```
