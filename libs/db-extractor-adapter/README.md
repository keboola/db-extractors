# DB extractor adapter

This library contains a common interface for connecting to and data extracting from, various sources:
 - It is intended for use with [db-extractor-common](https://github.com/keboola/db-extractor-common).
 - It supports **PDO** and **ODBC** connections for now.
 - The interfaces defined in this library can be easily used to support other methods, e.g. cli BCP tool.
 
## Main Classes

- **Interface [`DbConnection`](https://github.com/keboola/db-extractor-adapter/blob/master/src/Connection/DbConnection.php)** is an abstraction that represents a connection to the database.
    - Abstract class [`BaseDbConnection`](https://github.com/keboola/db-extractor-adapter/blob/master/src/Connection/BaseDbConnection.php) contains common code and retry mechanisms.
    - Class [`PdoDbConnection`](https://github.com/keboola/db-extractor-adapter/blob/master/src/PDO/PdoConnection.php) implements connection using PDO extension. 
    - Class [`OdbcDbConnection`](https://github.com/keboola/db-extractor-adapter/blob/master/src/ODBC/OdbcConnection.php) implements connection using ODBC extension.
- **Interface [`QueryResult`](https://github.com/keboola/db-extractor-adapter/blob/master/src/ValueObject/QueryResult.php)** is an abstraction that represents query result - rows returned from database.
    - Class [`PdoQueryResult`](https://github.com/keboola/db-extractor-adapter/blob/master/src/PDO/PdoQueryResult.php) represents result from PDO connection.
    - Class [`OdbcQueryResult`](https://github.com/keboola/db-extractor-adapter/blob/master/src/ODBC/OdbcQueryResult.php) represents result from ODBC connection.
- **Interface [`ExportAdapter`](https://github.com/keboola/db-extractor-adapter/blob/master/src/ExportAdapter.php)**  is an abstraction which defines how the data is to be extracted.
    - Based on `ExportConfig`, [`ExportResult`](https://github.com/keboola/db-extractor-adapter/blob/master/src/ValueObject/ExportResult.php) is generated. The rows are written to the specified CSV file.
    - By implementing this interface, it is possible to add support for CLI tools for export.
    - Abstract class [`BaseExportAdapter`](https://github.com/keboola/db-extractor-adapter/blob/master/src/BaseExportAdapter.php) contains common code.
    - Class [`PdoExportAdapter`](https://github.com/keboola/db-extractor-adapter/blob/master/src/PDO/PdoExportAdapter.php) implements export for PDO connection.
    - Class [`OdbcExportAdapter`](https://github.com/keboola/db-extractor-adapter/blob/master/src/ODBC/OdbcExportAdapter.php) implements export for ODBC connection.
    - Class [`FallbackExportAdapter`](https://github.com/keboola/db-extractor-adapter/blob/master/src/FallbackExportAdapter.php) allows you to use multiple adapters. If one fails, then fallback adapter is used.
- **Interface [`QueryFactory`](https://github.com/keboola/db-extractor-adapter/blob/master/src/Query/QueryFactory.php)** used to generate SQL query from `ExportConfig`. It is used if query is not set in the config.
    Class [`DefaultQueryFactory`](https://github.com/keboola/db-extractor-adapter/blob/master/src/Query/DefaultQueryFactory.php) is base implementation for MySQL/MariaDb compatible SQL dialects. 
- **Class [`QueryResultCsvWriter`](https://github.com/keboola/db-extractor-adapter/blob/master/src/QueryResultCsvWriter.php)** used to write rows from the `QueryResult` to the specified CSV file. 

## Development

Clone this repository and init the workspace with following command:

```
git clone https://github.com/keboola/db-extractor-adapter
cd db-extractor-adapter
docker-compose build
docker-compose run --rm dev composer install --no-scripts
```

Run the test suite using this command:

```
docker-compose run --rm dev composer tests
```
