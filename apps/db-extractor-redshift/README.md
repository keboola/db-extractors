[![Docker Repository on Quay](https://quay.io/repository/keboola/db-extractor-redshift/status "Docker Repository on Quay")](https://quay.io/repository/keboola/db-extractor-redshift)
[![Build Status](https://travis-ci.org/keboola/db-extractor-redshift.svg?branch=master)](https://travis-ci.org/keboola/db-extractor-redshift)

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

1. Create Redshift cluster and S3 bucket from CloudFormation template `aws-services.json`
2. Copy set-env.template.sh to set-env.sh and fill in you Redshift and S3 credentials
3. Run the tests:

```
source set-env.sh && docker-compose run --rm app
```
