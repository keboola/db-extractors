<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests\Traits;

trait ConfigTrait
{
    public function getConfig(): array
    {
        $configTemplate = <<<JSON
{
  "parameters": {
    "data_dir": "/code/tests/phpunit/data",
    "name": "sales",
    "query": "SELECT * FROM \"escaping\"",
    "outputTable": "in.c-main.escaping",
    "columns": [],
    "primaryKey": [],
    "retries": 3,
    "db": {
      "host": "%s",
      "port": "%s",
      "user": "%s",
      "#password": "%s",
      "database": "%s",
      "ssh": {
        "enabled": true,
        "keys": {
          "#private": %s,
          "public": %s
        },
        "user": "root",
        "sshHost": "sshproxy",
        "remoteHost": "ORACLE",
        "remotePort": "1433",
        "localPort": "12345"
      }
    }
  }
}
JSON;

        $configJson = sprintf(
            $configTemplate,
            getenv('ORACLE_DB_HOST'),
            getenv('ORACLE_DB_PORT'),
            getenv('ORACLE_DB_USER'),
            getenv('ORACLE_DB_PASSWORD'),
            getenv('ORACLE_DB_DATABASE'),
            json_encode($this->getPrivateKey()),
            json_encode($this->getPublicKey())
        );

        return json_decode($configJson, true);
    }

    public function getConfigConnection(): array
    {
        $configTemplate = <<<JSON
{
  "action": "testConnection",
  "parameters": {
    "data_dir": "/code/tests/phpunit/data",
    "db": {
      "host": "%s",
      "port": "%s",
      "user": "%s",
      "#password": "%s",
      "database": "%s",
      "ssh": {
        "enabled": true,
        "keys": {
          "#private": %s,
          "public": %s
        },
        "user": "root",
        "sshHost": "sshproxy",
        "remoteHost": "ORACLE",
        "remotePort": "1433",
        "localPort": "12345"
      }
    }
  }
}
JSON;

        $configJson = sprintf(
            $configTemplate,
            getenv('ORACLE_DB_HOST'),
            getenv('ORACLE_DB_PORT'),
            getenv('ORACLE_DB_USER'),
            getenv('ORACLE_DB_PASSWORD'),
            getenv('ORACLE_DB_DATABASE'),
            json_encode($this->getPrivateKey()),
            json_encode($this->getPublicKey())
        );

        return json_decode($configJson, true);
    }

    public function getPrivateKey(): string
    {
        return (string) file_get_contents('/root/.ssh/id_rsa');
    }

    public function getPublicKey(): string
    {
        return (string) file_get_contents('/root/.ssh/id_rsa.pub');
    }
}
