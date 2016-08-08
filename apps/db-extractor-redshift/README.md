[![Docker Repository on Quay](https://quay.io/repository/keboola/db-extractor-redshift/status "Docker Repository on Quay")](https://quay.io/repository/keboola/db-extractor-redshift)

# Redshift DB Extractor

## Example configuration

    {
      "db": {
        "driver": "redshift",
        "host": "HOST",
        "port": "PORT",
        "database": "DATABASE",
        "user": "USERNAME",
        "password": "PASSWORD",
        "ssh": {
          "enabled": true,
          "keys": {
            "private": "ENCRYPTED_PRIVATE_SSH_KEY",
            "public": "PUBLIC_SSH_KEY"
          },
          "sshHost": "PROXY_HOSTNAME"
        }
      },
      "tables": [
        {
          "id": 1,
          "name": "employees",
          "query": "SELECT * FROM employees",
          "outputTable": "in.c-main.employees",
          "incremental": false,
          "enabled": true,
          "primaryKey": null
        }
      ]
    }

## Running Tests

To run tests, copy set-env.template.sh to set-env.sh and fill in your values for the environment variables. Then:

```
source set-env.sh && docker-compose run --rm app
```
