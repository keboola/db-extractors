<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Component\Logger;
use Keboola\DbExtractor\Extractor\MySQL;
use Keboola\DbExtractor\FunctionalTests\PdoTestConnection;
use Keboola\DbExtractor\Tests\Traits\ConfigTrait;
use Keboola\DbExtractor\TraitTests\RemoveAllTablesTrait;
use Keboola\DbExtractor\TraitTests\Tables\AutoIncrementTableTrait;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use PDO;
use PHPUnit\Framework\TestCase;

class QueryGenerationTest extends TestCase
{
    use AutoIncrementTableTrait;
    use ConfigTrait;
    use RemoveAllTablesTrait;

    protected PDO $connection;

    protected string $dataDir = __DIR__ . '/../data';

    public function setUp(): void
    {
        parent::setUp();
        $this->connection = PdoTestConnection::createConnection();
        $this->removeAllTables();
    }
    /**
     * @dataProvider simpleTableColumnsDataProvider
     */
    public function testGetSimplifiedPdoQuery(array $params, array $state, string $expected): void
    {
        $this->createAITable();
        $this->generateAIRows();
        $this->addAIConstraint();

        $config = $this->getRowConfig();
        $config['parameters'] = array_merge($config['parameters'], $params);
        $config['parameters']['query'] = empty($config['parameters']['table']) ?
            $config['parameters']['query'] : null;

        $exportConfig = ExportConfig::fromArray($config['parameters']);
        $extractor = new MySQL($config['parameters'], $state, new Logger(), $config['action'] ?? 'run');
        if ($exportConfig->isIncrementalFetching()) {
            $extractor->validateIncrementalFetching($exportConfig);
        }
        $query = $extractor->simpleQuery($exportConfig);
        $this->assertEquals($expected, $query);
    }

    public function simpleTableColumnsDataProvider(): array
    {
        return [
            // simple table select with all columns
            [
                [
                    'table' => [
                        'tableName' => 'test',
                        'schema' => 'testSchema',
                    ],
                    'columns' => [],
                ],
                [],
                'SELECT * FROM `testSchema`.`test`',
            ],
            // simple table select with all columns (columns as null)
            [
                [
                    'table' => [
                        'tableName' => 'test',
                        'schema' => 'testSchema',
                    ],
                    'columns' => [],
                ],
                [],
                'SELECT * FROM `testSchema`.`test`',
            ],
            // simple table with 2 columns selected
            [
                [
                    'table' => [
                        'tableName' => 'test',
                        'schema' => 'testSchema',
                    ],
                    'columns' => ['col1', 'col2'],
                ],
                [],
                'SELECT `col1`, `col2` FROM `testSchema`.`test`',
            ],
            // test simplePDO query with limit and timestamp column but no state
            [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'testdb',
                    ],
                    'columns' => [],
                    'incremental' => true,
                    'incrementalFetchingLimit' => 10,
                    'incrementalFetchingColumn' => 'datetime',
                ],
                [],
                'SELECT * FROM `testdb`.`auto Increment Timestamp` ORDER BY `datetime` LIMIT 10',
            ],
            // test simplePDO query with limit and idp column and previos state
            [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'testdb',
                    ],
                    'columns' => [],
                    'incremental' => true,
                    'incrementalFetchingLimit' => 10,
                    'incrementalFetchingColumn' => '_Weir%d I-D',
                ],
                [
                    'lastFetchedRow' => 4,
                ],
                "SELECT * FROM `testdb`.`auto Increment Timestamp` WHERE `_Weir%d I-D` >= '4' ORDER BY `_Weir%d I-D` LIMIT 10",
            ],
            // test simplePDO query timestamp column but no state and no limit
            [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'testdb',
                    ],
                    'columns' => [],
                    'incremental' => true,
                    'incrementalFetchingLimit' => null,
                    'incrementalFetchingColumn' => 'datetime',
                ],
                [],
                'SELECT * FROM `testdb`.`auto Increment Timestamp` ORDER BY `datetime`',
            ],
            // test simplePDO query id column and previous state and no limit
            [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'testdb',
                    ],
                    'columns' => [],
                    'incremental' => true,
                    'incrementalFetchingLimit' => null,
                    'incrementalFetchingColumn' => '_Weir%d I-D',
                ],
                [
                    'lastFetchedRow' => 4,
                ],
                "SELECT * FROM `testdb`.`auto Increment Timestamp` WHERE `_Weir%d I-D` >= '4' ORDER BY `_Weir%d I-D`",
            ],
        ];
    }
}
