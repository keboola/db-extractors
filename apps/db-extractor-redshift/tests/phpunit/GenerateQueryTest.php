<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Component\Logger;
use Keboola\DbExtractor\Extractor\Redshift;
use Keboola\DbExtractor\Extractor\RedshiftPdoConnection;
use Keboola\DbExtractor\Extractor\RedshiftQueryFactory;
use Keboola\DbExtractor\FunctionalTests\PdoTestConnection;
use Keboola\DbExtractor\Tests\Traits\ConfigTrait;
use Keboola\DbExtractor\TraitTests\RemoveAllTablesTrait;
use Keboola\DbExtractor\TraitTests\Tables\AutoIncrementTableTrait;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use PDO;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class GenerateQueryTest extends TestCase
{
    use AutoIncrementTableTrait;
    use RemoveAllTablesTrait;
    use ConfigTrait;

    protected PDO $connection;

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

        $params['outputTable'] = 'test';
        $params['query'] = '';
        $params['primaryKey'] = [];
        $params['retries'] = 3;
        $config = $this->getRowConfig();
        $config['parameters'] = array_merge($config['parameters'], $params);

        $exportConfig = ExportConfig::fromArray($config['parameters']);

        $queryFactory = new RedshiftQueryFactory($state);
        if (isset($state['lastFetchedRow']) && is_numeric($state['lastFetchedRow'])) {
            $queryFactory->setIncrementalFetchingColType(Redshift::INCREMENT_TYPE_NUMERIC);
        }

        $dsn = sprintf(
            'pgsql:dbname=%s;port=%s;host=%s',
            getenv('REDSHIFT_DB_DATABASE'),
            getenv('REDSHIFT_DB_PORT'),
            getenv('REDSHIFT_DB_HOST')
        );

        $query = $queryFactory->create(
            $exportConfig,
            new RedshiftPdoConnection(
                new NullLogger(),
                $dsn,
                (string) getenv('REDSHIFT_DB_USER'),
                (string) getenv('REDSHIFT_DB_PASSWORD'),
                []
            )
        );
        $this->assertEquals($expected, $query);
    }

    public function simpleTableColumnsDataProvider(): array
    {
        return [
            'simple table select with no column metadata' => [
                [
                    'table' => [
                        'tableName' => 'test',
                        'schema' => 'testSchema',
                    ],
                    'columns' => [],
                ],
                [],
                'SELECT * FROM "testSchema"."test"',
            ],
            'simple table with 2 columns selected' => [
                [
                    'table' => [
                        'tableName' => 'test',
                        'schema' => 'testSchema',
                    ],
                    'columns' => [
                        'col1',
                        'col2',
                    ],
                ],
                [],
                'SELECT "col1", "col2" FROM "testSchema"."test"',
            ],
            'test simplePDO query with limit and datetime column but no state' => [
                [
                    'table' => [
                        'tableName' => 'auto increment timestamp',
                        'schema' => 'public',
                    ],
                    'columns' => [
                        '_weir%d i-d',
                        'weir%d na-me',
                        'decimalcolumn',
                        'datetime',
                    ],
                    'incrementalFetchingLimit' => 10,
                    'incrementalFetchingColumn' => 'datetime',
                ],
                [],
                'SELECT "_weir%d i-d", "weir%d na-me", "decimalcolumn", "datetime"' .
                ' FROM "public"."auto increment timestamp"' .
                ' ORDER BY "datetime" LIMIT 10',
            ],
            'test simplePDO query with limit and idp column and previos state' => [
                [
                    'table' => [
                        'tableName' => 'auto increment timestamp',
                        'schema' => 'public',
                    ],
                    'columns' => [
                        '_weir%d i-d',
                        'weir%d na-me',
                        'decimalcolumn',
                        'datetime',
                    ],
                    'incrementalFetchingLimit' => 10,
                    'incrementalFetchingColumn' => '_weir%d i-d',
                ],
                [
                    'lastFetchedRow' => 4,
                ],
                'SELECT "_weir%d i-d", "weir%d na-me", "decimalcolumn", "datetime"' .
                ' FROM "public"."auto increment timestamp"' .
                ' WHERE "_weir%d i-d" >= 4' .
                ' ORDER BY "_weir%d i-d" LIMIT 10',
            ],
            'test simplePDO query datetime column but no state and no limit' => [
                [
                    'table' => [
                        'tableName' => 'auto increment timestamp',
                        'schema' => 'public',
                    ],
                    'columns' => [
                        '_weir%d i-d',
                        'weir%d na-me',
                        'decimalcolumn',
                        'datetime',
                    ],
                    'incrementalFetchingLimit' => null,
                    'incrementalFetchingColumn' => 'datetime',
                ],
                [],
                'SELECT "_weir%d i-d", "weir%d na-me", "decimalcolumn", "datetime"' .
                ' FROM "public"."auto increment timestamp"' .
                ' ORDER BY "datetime"',
            ],
            'test simplePDO query datetime column and previos state and limit' => [
                [
                    'table' => [
                        'tableName' => 'auto increment timestamp',
                        'schema' => 'public',
                    ],
                    'columns' => [
                        '_weir%d i-d',
                        'weir%d na-me',
                        'decimalcolumn',
                        'datetime',
                    ],
                    'incrementalFetchingLimit' => 1000,
                    'incrementalFetchingColumn' => 'datetime',
                ],
                [
                    'lastFetchedRow' => '2018-10-26 10:52:32',
                ],
                'SELECT "_weir%d i-d", "weir%d na-me", "decimalcolumn", "datetime"' .
                ' FROM "public"."auto increment timestamp"' .
                ' WHERE "datetime" >= \'2018-10-26 10:52:32\'' .
                ' ORDER BY "datetime"' .
                ' LIMIT 1000',
            ],
        ];
    }
}
