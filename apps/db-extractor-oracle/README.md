# Oracle DB Extractor

## Development
 
Clone this repository and init the workspace with following command:

```
git clone https://github.com/keboola/db-extractor-oracle
cd db-extractor-oracle
docker-compose build
docker-compose run --rm dev composer install --no-scripts
```

Run the test suite using this command:

```
docker-compose run --rm dev composer tests
```
