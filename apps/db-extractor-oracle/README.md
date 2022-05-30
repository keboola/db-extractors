# Oracle DB Extractor

## Configuration
The configuration `config.json` contains following properties in `parameters` key:
- `db`
    - `host` - string - one of [`host` and `port`] or `tnsnames` must be configured
    - `port` - int - one of [`host` and `port`] or `tnsnames` must be configured
    - `tnsnames` - string - one of [`host` and `port`] or `tnsnames` must be configured
    - `database` - string (required)
    - `user` - string (required)
    - `\#password` - string (required)
    - `connectThrough` - bool (optional, default `false`)
        - If enabled:
            - Value from the `KBC_REALUSER` environment variable is used as the `OracleConnection.PROXY_USER_NAME`.
            - if `KBC_REALUSER` is not set, a `UserException` is thrown.
        - To use this feature:
            - The SAML login to the Keboola Connection must be used.
            - The SAML token must contain the required data and the stack must be set correctly.
    - `ssh` - array (optional, but if present, enabled, keys/public, user, and sshHost are required)
        - `enabled` - boolean
        - `keys` - array
            - `\#private` - string 
            - `public` - string
        - `sshHost` - string
        - `sshPort` - int
        - `remotePort` - string
        - `localPort` - string
        - `user` - string
        - `compression` - boolean
- `id` - string (required)
- `name` - string (required)
- `query` - string - one of `query` or `table` must be configured
- `table` - array - one of `query` or `table` must be configured
    - `schema` - string (required)
    - `tableName` - string (required)
- `columns` - array of strings
- `outputTable` – string (required)
- `incremental` - boolean (optional)
- `incrementalFetchingColumn` - string (optional)
- `incrementalFetchingLimit` - int (optional)
- `enabled` - boolean (optional)
- `primaryKey` - array of strings (optional)
- `retries` - int (optional) number of times to retry failures

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

## License

MIT licensed, see [LICENSE](./LICENSE) file.
