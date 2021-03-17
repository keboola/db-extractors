<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\CommonExceptions\UserExceptionInterface;
use Keboola\Component\Logger;
use Keboola\DbExtractor\Configuration\NodeDefinition\MysqlDbNode;
use Keboola\DbExtractor\FunctionalTests\PdoTestConnection;
use Keboola\DbExtractor\MySQLApplication;
use Keboola\DbExtractor\Tests\Traits\ConfigTrait;
use Keboola\DbExtractor\Tests\Traits\ExpectedColumnsTrait;
use Keboola\DbExtractor\TraitTests\AddConstraintTrait;
use Keboola\DbExtractor\TraitTests\CreateTableTrait;
use Keboola\DbExtractor\TraitTests\InsertRowsTrait;
use Keboola\DbExtractor\TraitTests\RemoveAllTablesTrait;
use Keboola\DbExtractor\TraitTests\Tables\AutoIncrementTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\EmojiTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\EscapingTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\SalesTableTrait;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorConfig\Exception\UserException as ConfigUserException;
use Nette\Utils;
use \PDO;
use \Generator;

class MySQLTest extends TestCase
{
    use CreateTableTrait;
    use InsertRowsTrait;
    use AddConstraintTrait;
    use SalesTableTrait;
    use EmojiTableTrait;
    use EscapingTableTrait;
    use AutoIncrementTableTrait;
    use ConfigTrait;
    use RemoveAllTablesTrait;
    use ExpectedColumnsTrait;

    protected string $dataDir = __DIR__ . '/../data';

    protected PDO $connection;

    protected function setUp(): void
    {
        parent::setUp();
        $this->connection = PdoTestConnection::createConnection();
        $this->removeAllTables();
    }

    /**
     * @dataProvider configProvider
     */
    public function testManifestMetadata(array $config): void
    {
        $this->createAITable();
        $this->generateAIRows();

        $this->createTable('auto Increment Timestamp FK', [
            'some_primary_key' => 'INT NOT NULL AUTO_INCREMENT COMMENT \'This is a weird ID\' PRIMARY KEY',
            'random_name' => 'VARCHAR(30) NOT NULL DEFAULT \'pam\' COMMENT \'This is a weird name\'',
            'datetime' => 'DATETIME DEFAULT CURRENT_TIMESTAMP',
            'foreign_key' => 'INT COMMENT \'This is a foreign key\'',
        ]);
        $this->addConstraint(
            'auto Increment Timestamp FK',
            $this->quoteIdentifier('foreign_keyFK'),
            'FOREIGN KEY',
            $this->quoteIdentifier('foreign_key'),
            '`auto Increment Timestamp` (`_Weir%d I-D`) ON DELETE CASCADE'
        );
        $this->insertRows(
            'auto Increment Timestamp FK',
            ['random_name', 'foreign_key'],
            [['sue', 1]]
        );

        $isConfigRow = !isset($config['parameters']['tables']);

        $tableParams = ($isConfigRow) ? $config['parameters'] : $config['parameters']['tables'][0];
        unset($tableParams['query']);
        $tableParams['outputTable'] = 'in.c-main.foreignkey';
        $tableParams['primaryKey'] = ['some_primary_key'];
        $tableParams['table'] = [
            'tableName' => 'auto Increment Timestamp FK',
            'schema' => 'test',
        ];
        if ($isConfigRow) {
            $config['parameters'] = $tableParams;
        } else {
            $config['parameters']['tables'][0] = $tableParams;
            unset($config['parameters']['tables'][1]);
            unset($config['parameters']['tables'][2]);
        }

        $app = new MySQLApplication($config, new Logger(), [], $this->dataDir);

        $result = $app->run();

        $importedTable = ($isConfigRow) ? $result['imported']['outputTable'] : $result['imported'][0]['outputTable'];

        $sanitizedTable = Utils\Strings::webalize($importedTable, '._');
        $outputManifest = json_decode(
            (string) file_get_contents($this->dataDir . '/out/tables/' . $sanitizedTable . '.csv.manifest'),
            true
        );

        $this->assertArrayHasKey('destination', $outputManifest);
        $this->assertArrayHasKey('incremental', $outputManifest);
        $this->assertArrayHasKey('metadata', $outputManifest);
        $expectedMetadata = [
            'KBC.name' => 'auto Increment Timestamp FK',
            'KBC.sanitizedName' => 'auto_Increment_Timestamp_FK',
            'KBC.schema' => 'test',
            'KBC.type' => 'BASE TABLE',
            'KBC.rowCount' => 1,
        ];
        $tableMetadata = [];
        foreach ($outputManifest['metadata'] as $i => $metadata) {
            $this->assertArrayHasKey('key', $metadata);
            $this->assertArrayHasKey('value', $metadata);
            $tableMetadata[$metadata['key']] = $metadata['value'];
        }
        $this->assertEquals($expectedMetadata, $tableMetadata);

        $this->assertArrayHasKey('column_metadata', $outputManifest);
        $this->assertCount(4, $outputManifest['column_metadata']);

        $expectedColumnMetadata = [
            'some_primary_key' =>
                [
                    [
                        'key' => 'KBC.datatype.type',
                        'value' => 'int',
                    ],
                    [
                        'key' => 'KBC.datatype.nullable',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'INTEGER',
                    ],
                    [
                        'key' => 'KBC.datatype.length',
                        'value' => '10',
                    ],
                    [
                        'key' => 'KBC.sourceName',
                        'value' => 'some_primary_key',
                    ],
                    [
                        'key' => 'KBC.sanitizedName',
                        'value' => 'some_primary_key',
                    ],
                    [
                        'key' => 'KBC.primaryKey',
                        'value' => true,
                    ],
                    [
                        'key' => 'KBC.uniqueKey',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.ordinalPosition',
                        'value' => 1,
                    ],
                    [
                        'key' => 'KBC.autoIncrement',
                        'value' => true,
                    ],
                    [
                        'key' => 'KBC.autoIncrementValue',
                        'value' => 2,
                    ],
                    [
                        'key' => 'KBC.description',
                        'value' => 'This is a weird ID',
                    ],
                    [
                        'key' => 'KBC.constraintName',
                        'value' => 'PRIMARY',
                    ],
                ],
            'random_name' =>
                [
                    [
                        'key' => 'KBC.datatype.type',
                        'value' => 'varchar',
                    ],
                    [
                        'key' => 'KBC.datatype.nullable',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'STRING',
                    ],
                    [
                        'key' => 'KBC.datatype.length',
                        'value' => '30',
                    ],
                    [
                        'key' => 'KBC.datatype.default',
                        'value' => 'pam',
                    ],
                    [
                        'key' => 'KBC.sourceName',
                        'value' => 'random_name',
                    ],
                    [
                        'key' => 'KBC.sanitizedName',
                        'value' => 'random_name',
                    ],
                    [
                        'key' => 'KBC.primaryKey',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.uniqueKey',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.ordinalPosition',
                        'value' => '2',
                    ],
                    [
                        'key' => 'KBC.description',
                        'value' => 'This is a weird name',
                    ],
                ],
            'datetime' =>
                [
                    [
                        'key' => 'KBC.datatype.type',
                        'value' => 'datetime',
                    ],
                    [
                        'key' => 'KBC.datatype.nullable',
                        'value' => true,
                    ],
                    [
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'TIMESTAMP',
                    ],
                    [
                        'key' => 'KBC.datatype.default',
                        'value' => 'CURRENT_TIMESTAMP',
                    ],
                    [
                        'key' => 'KBC.sourceName',
                        'value' => 'datetime',
                    ],
                    [
                        'key' => 'KBC.sanitizedName',
                        'value' => 'datetime',
                    ],
                    [
                        'key' => 'KBC.primaryKey',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.uniqueKey',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.ordinalPosition',
                        'value' => '3',
                    ],
                ],
            'foreign_key' =>
                [
                    [
                        'key' => 'KBC.datatype.type',
                        'value' => 'int',
                    ],
                    [
                        'key' => 'KBC.datatype.nullable',
                        'value' => true,
                    ],
                    [
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'INTEGER',
                    ],
                    [
                        'key' => 'KBC.datatype.length',
                        'value' => '10',
                    ],
                    [
                        'key' => 'KBC.datatype.default',
                        'value' => '',
                    ],
                    [
                        'key' => 'KBC.sourceName',
                        'value' => 'foreign_key',
                    ],
                    [
                        'key' => 'KBC.sanitizedName',
                        'value' => 'foreign_key',
                    ],
                    [
                        'key' => 'KBC.primaryKey',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.uniqueKey',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.ordinalPosition',
                        'value' => '4',
                    ],
                    [
                        'key' => 'KBC.description',
                        'value' => 'This is a foreign key',
                    ],
                    [
                        'key' => 'KBC.foreignKey',
                        'value' => true,
                    ],
                    [
                        'key' => 'KBC.foreignKeyName',
                        'value' => 'foreign_keyFK',
                    ],
                    [
                        'key' => 'KBC.foreignKeyRefSchema',
                        'value' => 'test',
                    ],
                    [
                        'key' => 'KBC.foreignKeyRefTable',
                        'value' => 'auto Increment Timestamp',

                    ],
                    [
                        'key' => 'KBC.foreignKeyRefColumn',
                        'value' => '_Weir%d I-D',
                    ],
                    [
                        'key' => 'KBC.constraintName',
                        'value' => 'foreign_keyFK',
                    ],
                ],
        ];
        $this->assertEquals($expectedColumnMetadata, $outputManifest['column_metadata']);
    }

    public function testSchemaNotEqualToDatabase(): void
    {
        $this->createSalesTable('ext_sales');
        $this->generateSalesRows('ext_sales');

        $config = $this->getConfig();

        $config['parameters']['tables'][2]['table'] = ['schema' => 'temp_schema', 'tableName' => 'ext_sales'];
        unset($config['parameters']['tables'][0]);
        unset($config['parameters']['tables'][1]);

        try {
            $app = new MySQLApplication($config, new Logger(), [], $this->dataDir);
            $app->run();
            $this->fail('table schema and database mismatch');
        } catch (UserException $e) {
            $this->assertStringStartsWith('Invalid Configuration', $e->getMessage());
        }
    }

    public function testThousandsOfTables(): void
    {
        $countTables = 2000;
        for ($i = 0; $i < $countTables; $i++) {
            $this->createSalesTable('sales_' . $i);
        }

        $config = $this->getConfig();
        $config['action'] = 'getTables';
        $app = new MySQLApplication($config, new Logger(), [], $this->dataDir);

        $result = $app->run();
        Assert::assertCount($countTables, $result['tables']);
    }

    public function testWeirdColumnNames(): void
    {
        $this->createAITable();
        $this->generateAIRows();

        $config = $this->getIncrementalConfig();

        $app = new MySQLApplication($config, new Logger(), [], $this->dataDir);
        $result = $app->run();

        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 6,
            ],
            $result['imported']
        );
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported']['outputTable'] . '.csv.manifest';
        $manifest = json_decode((string) file_get_contents($outputManifestFile), true);
        $expectedColumns = ['Weir_d_I_D', 'Weir_d_Na_me', 'someInteger', 'someDecimal', 'type', 'datetime'];
        $this->assertEquals($expectedColumns, $manifest['columns']);
        $this->assertEquals(['Weir_d_I_D'], $manifest['primary_key']);
    }

    public function testRunWithNetworkCompression(): void
    {
        $this->createAITable();
        $this->generateAIRows();

        $config = $this->getIncrementalConfig();
        $config['parameters']['db']['networkCompression'] = true;
        $app = new MySQLApplication($config, new Logger(), [], $this->dataDir);
        $result = $app->run();
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 6,
            ],
            $result['imported']
        );

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(6, $result['state']['lastFetchedRow']);
    }

    public function testDBSchemaMismatchConfigRowWithNoName(): void
    {
        $config = $this->getRowConfig();
        // select a table from a different schema
        unset($config['parameters']['query']);
        $config['parameters']['table'] = [
            'tableName' => 'ext_sales',
            'schema' => 'temp_schema',
        ];
        try {
            $app = new MySQLApplication($config, new Logger(), [], $this->dataDir);
            $app->run();
            $this->fail('Should throw a user exception.');
        } catch (UserException $e) {
            $this->assertStringStartsWith('Invalid Configuration [ext_sales]', $e->getMessage());
        }
    }

    public function testTestIgnoringExtraKeys(): void
    {
        $this->createEscapingTable();
        $this->generateEscapingRows();
        $config = $this->getRowConfig();
        $config['parameters']['someExtraKey'] = 'test';
        $app = new MySQLApplication($config, new Logger(), [], $this->dataDir);
        $result = $app->run();

        $this->assertEquals('success', $result['status']);
    }

    public function testIncrementalNotPresentNoResults(): void
    {
        $this->createSalesTable();
        $this->generateSalesRows();

        $config = $this->getRowConfig();
        unset($config['parameters']['incremental']);
        $config['parameters']['query'] = 'SELECT * FROM sales WHERE 1 = 2;'; // no results
        $app = new MySQLApplication($config, new Logger(), [], $this->dataDir);
        $result = $app->run();

        $this->assertEquals('success', $result['status']);
    }

    public function testMultipleForeignKeysOnOneColumn(): void
    {
        $columns = [
            'id' => 'INT NOT NULL AUTO_INCREMENT PRIMARY KEY',
            'value' => 'VARCHAR(30) NOT NULL',
        ];
        $this->createTable('pk_fk_target_table1', $columns);
        $this->createTable('pk_fk_target_table2', $columns);

        $this->createTable('pk_fk_table', ['id' => 'INT NOT NULL PRIMARY KEY']);

        $this->addConstraint(
            'pk_fk_table',
            'pk_fk_target_table1',
            'FOREIGN KEY',
            'id',
            'pk_fk_target_table1(id)'
        );

        $this->addConstraint(
            'pk_fk_table',
            'pk_fk_target_table2',
            'FOREIGN KEY',
            'id',
            'pk_fk_target_table2(id)'
        );

        $this->insertRows('pk_fk_target_table1', ['id', 'value'], [[123, 'test']]);
        $this->insertRows('pk_fk_target_table2', ['id', 'value'], [[123, 'test']]);
        $this->insertRows('pk_fk_table', ['id'], [[123]]);

        $config = $this->getConfig();
        $config['action'] = 'getTables';
        $app = new MySQLApplication($config, new Logger(), [], $this->dataDir);
        $result = $app->run();

        $this->assertEquals([
            [
                'name' => 'pk_fk_table',
                'schema' => 'test',
                'columns' =>
                    [
                        [
                            'name' => 'id',
                            'type' => 'int',
                            'primaryKey' => true,
                        ],
                    ],
            ],
            [
                'name' => 'pk_fk_target_table1',
                'schema' => 'test',
                'columns' =>
                    [
                        [
                            'name' => 'id',
                            'type' => 'int',
                            'primaryKey' => true,
                        ],
                        [
                            'name' => 'value',
                            'type' => 'varchar',
                            'primaryKey' => false,
                        ],
                    ],
            ],
            [
                'name' => 'pk_fk_target_table2',
                'schema' => 'test',
                'columns' =>
                    [
                        [
                            'name' => 'id',
                            'type' => 'int',
                            'primaryKey' => true,
                        ],
                        [
                            'name' => 'value',
                            'type' => 'varchar',
                            'primaryKey' => false,
                        ],
                    ],
            ],
        ], $result['tables']);
    }

    public function testGetTablesWithoutListColumnsPermissions(): void
    {
        try {
            $this->connection->query("DROP USER 'user_no_perms'");
        } catch (\PDOException $e) {
            // ignore, the user does not have to exist
        }
        $this->connection->query(
            "CREATE USER 'user_no_perms' IDENTIFIED BY 'password'"
        );

        // With this privilege can user list table names but not names of the columns
        // In MySQLMetadataProvider must be this situation properly handled
        $this->connection->query("GRANT CREATE ON test.* TO 'user_no_perms'");

        $this->createAITable();
        $this->createEscapingTable();
        $this->createEmojiTable();

        $config = $this->getConfig();
        $config['parameters']['db']['user'] = 'user_no_perms';
        $config['parameters']['db']['#password'] = 'password';
        $config['action'] = 'getTables';
        $app = new MySQLApplication($config, new Logger(), [], $this->dataDir);

        $result = $app->run();

        $this->assertEquals('success', $result['status']);
        $this->assertEquals([], $result['tables']);
    }

    public function testRunTableSchemaCaseInsensitive(): void
    {
        $this->createAITable();
        $this->generateAIRows();

        $config = $this->getRowConfig();
        unset($config['parameters']['query']);
        $config['parameters']['table'] = [
            'schema' => 'TesT',
            'tableName' => 'Auto_INCREMENT_TimestamP',
        ];

        $app = new MySQLApplication($config, new Logger(), [], $this->dataDir);

        // The export should crash on a database error
        // MySQL case sensitive settings vary by platform: https://stackoverflow.com/a/6134059
        // "Database and table names are not case sensitive in Windows, and case sensitive in most varieties of Unix."
        // So in PHP all check must be done case-insensitive
        try {
            $app->run();
            Assert::fail('Exception expected.');
        } catch (UserExceptionInterface $e) {
            Assert::assertThat($e->getMessage(), Assert::logicalOr(
                // Message differs between MySQL versions
                Assert::stringContains(
                    "Base table or view not found: 1146 Table 'TesT.Auto_INCREMENT_TimestamP' doesn't exist"
                ),
                Assert::stringContains("Unknown database 'TesT'")
            ));
        }
    }

    /**
     * @dataProvider transactionIsolationLevelProvider
     */
    public function testTransactionIsolationLevel(string $level): void
    {
        $this->createEscapingTable();
        $this->generateEscapingRows();

        $config = $this->getRowConfig();
        $config['parameters']['db']['transactionIsolationLevel'] = $level;

        $app = new MySQLApplication($config, new Logger(), [], $this->dataDir);
        $result = $app->run();

        Assert::assertArrayHasKey('status', $result);
        Assert::assertEquals('success', $result['status']);
    }

    public function transactionIsolationLevelProvider(): array
    {
        return [
            [MysqlDbNode::TRANSACTION_LEVEL_REPEATABLE_READ],
            [MysqlDbNode::TRANSACTION_LEVEL_READ_COMMITTED],
            [MysqlDbNode::TRANSACTION_LEVEL_READ_UNCOMMITTED],
            [MysqlDbNode::TRANSACTION_LEVEL_SERIALIZABLE],
        ];
    }

    public function testInvalidTransactionIsolationLevel(): void
    {
        $this->createEscapingTable();
        $this->generateEscapingRows();

        $config = $this->getRowConfig();
        $config['parameters']['db']['transactionIsolationLevel'] = 'invalid transaction level';

        $expctedMessage = 'The value "invalid transaction level" is not allowed for path "root.parameters.db.transactionIsolationLevel". ';
        $expctedMessage .= 'Permissible values: "REPEATABLE READ", "READ COMMITTED", "READ UNCOMMITTED", "SERIALIZABLE"';
        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage($expctedMessage);
        new MySQLApplication($config, new Logger(), [], $this->dataDir);
    }

    public function configProvider(): Generator
    {
        yield [
            $this->getRowConfig(),
        ];

        yield [
            $this->getConfig(),
        ];
    }
}
