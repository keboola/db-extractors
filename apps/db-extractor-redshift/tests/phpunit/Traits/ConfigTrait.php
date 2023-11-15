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
    "data_dir": "testDir",
    "tables": [
      {
        "id": 0,
        "name": "escaping",
        "query": "SELECT col1, col2, col3 FROM testing.escaping ORDER BY col3",
        "outputTable": "in.c-main.escaping",
        "incremental": true,
        "primaryKey": ["col3"],
        "enabled": true
      },
      {
        "id": 1,
        "name": "escapingEmpty",
        "query": "SELECT col1, col2 FROM testing.escaping LIMIT 0",
        "outputTable": "in.c-main.escapingEmpty",
        "incremental": false,
        "primaryKey": [],
        "enabled": true
      },
      {
        "id": 2,
        "name": "tableColumns",
        "table": {
          "schema": "testing",
          "tableName": "escaping"
        },
        "outputTable": "in.c-main.tableColumns",
        "incremental": false,
        "primaryKey": null,
        "enabled": true,
        "columns": ["col1", "col2"]
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
    "db": %s,
    "data_dir": "testDir",
    "table": {
      "schema": "testing",
      "tableName": "escaping"
    },
    "outputTable": "in.c-main.tableColumns",
    "incremental": false,
    "primaryKey": null,
    "enabled": true,
    "columns": ["col1", "col2"]
  }
}
JSON;
        return (array) json_decode(
            sprintf($configTemplate, json_encode(PdoTestConnection::getDbConfigArray())),
            true,
        );
    }
}
