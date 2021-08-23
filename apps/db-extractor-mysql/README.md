# MySQL DB Extractor
[![Build Status](https://travis-ci.com/keboola/db-extractor-mysql.svg?branch=master)](https://travis-ci.com/keboola/db-extractor-mysql)

### Development

- Clone the repository.
- Create `.env` file with `MYSQL_VERSION=latest`.
- Run `docker-compose build`.

#### Tools

- codesniffer: `docker-compose run --rm dev composer phpcs` 
- static analysis: `docker-compose run --rm dev composer phpstan`
- unit tests: `docker-compose run --rm dev composer tests`

#### Configuration Options

The configuration requires a `db` node with the following properties: 

- host: string (required) the hostname of the database
- port: numeric (required)
- user: string (required)
- \#password: string (required)
- networkCompression: boolean 
- ssl: array (optional, but if present, enabled, ca, cert, and key are required)
  - enabled: boolean 
  - ca: string
  - cert: string
  - key: string
  - cipher: string
  - verifyServerCert: boolean (default true)
- ssh: array (optional, but if present, enabled, keys/public, user, and sshHost are required)
  - enabled: boolean 
  - keys: array 
    - \#private: string
    - public: string                
  - user string
  - sshHost string,
  - sshPort string
- transactionIsolationLevel: enum (optional) - possible values `REPEATABLE READ`, `READ COMMITTED`, `READ UNCOMMITTED`, `SERIALIZABLE`
   
There are 2 possible types of table extraction.  
1. A table defined by `schema` and `tableName`, this option can also include a columns list.
2. A `query` which is the SQL SELECT statement to be executed to produce the result table.

The extraction has the following configuration options:

- id: numeric (required),
- name: string (required),
- query: stirng (optional, but required if table not present)
- table: array (optional, but required if table not present)
  - tableName: string
  - schema: string
- columns: array of strings (only for table type configurations)
- outputTable: string (required)
- incremental: boolean (optional)
- primaryKey: array of strings (optional)
- incrementalFetchingColumn: string (optional)
- incrementalFetchingLimit: integer (optional)
- enabled: boolean (optional)
- retries: integer (optional) number of times to retry failures
