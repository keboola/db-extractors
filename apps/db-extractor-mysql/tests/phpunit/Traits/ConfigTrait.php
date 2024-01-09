<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests\Traits;

use Keboola\DbExtractor\FunctionalTests\PdoTestConnection;

trait ConfigTrait
{
    private function getConfig(): array
    {
        $configTemplate = <<<JSON
{
  "parameters": {
    "db": %s,
    "data_dir": "MYSQL",
    "tables": [
      {
        "id": 1,
        "name": "sales",
        "query": "SELECT * FROM sales",
        "outputTable": "in.c-main.sales",
        "incremental": false,
        "primaryKey": null,
        "enabled": true,
        "advancedMode": true
      },
      {
        "id": 2,
        "name": "escaping",
        "query": "SELECT * FROM escaping",
        "outputTable": "in.c-main.escaping",
        "incremental": false,
        "primaryKey": [
          "orderId"
        ],
        "enabled": true,
        "advancedMode": true
      },
      {
        "id": 3,
        "enabled": true,
        "name": "tableColumns",
        "outputTable": "in.c-main.tableColumns",
        "incremental": false,
        "primaryKey": null,
        "table": {
          "schema": "test",
          "tableName": "sales"
        },
        "columns": [
          "usergender",
          "usercity",
          "usersentiment",
          "zipcode"
        ]
      }
    ]
  }
}
JSON;
        return (array) json_decode(
            sprintf($configTemplate, json_encode(PdoTestConnection::getDbConfigArray())),
            true,
        );
    }

    public function getRowConfig(): array
    {
        $configTemplate = <<<JSON
{
  "parameters": {
    "data_dir": "MYSQL",
    "db": %s,
    "query": "SELECT * FROM escaping",
    "outputTable": "in.c-main.escaping",
    "incremental": false,
    "primaryKey": ["col1"],
    "retries": 3
  }
}
JSON;
        return (array) json_decode(
            sprintf($configTemplate, json_encode(PdoTestConnection::getDbConfigArray())),
            true,
        );
    }


    public function getIncrementalConfig(): array
    {
        $configTemplate = <<<JSON
{
  "parameters": {
    "data_dir": "MYSQL",
    "db": %s,
    "table": {
        "tableName": "auto Increment Timestamp",
        "schema": "test"    
    },
    "name": "auto-increment-timestamp",
    "outputTable": "in.c-main.auto-increment-timestamp",
    "incremental": true,
    "primaryKey": ["_Weir%%d I-D"],
    "incrementalFetchingColumn": "_Weir%%d I-D",
    "retries": 3
  }
}
JSON;
        return (array) json_decode(
            sprintf($configTemplate, json_encode(PdoTestConnection::getDbConfigArray())),
            true,
        );
    }
}
