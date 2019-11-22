<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

class OracleIncrementalTest extends OracleBaseTest
{
    public function testIncrementalFetchingByAutoIncrement(): void
    {
        $config = $this->getIncrementalFetchingConfig(self::DRIVER);
        $this->createIncrementalFetchingTable($config);

        $result = $this->createApplication($config)->run();

        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2,
            ],
            $result['imported']
        );

        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(2, $result['state']['lastFetchedRow']);

        $noNewRowsResult = $this->createApplication($config, $result['state'])->run();
        $this->assertEquals(1, $noNewRowsResult['imported']['rows']);
        $this->assertEquals($result['state'], $noNewRowsResult['state']);

        $this->executeStatement(
            $this->connection,
            sprintf(
                'INSERT INTO %s ("name", "decimal") VALUES (\'leo\', 50.89247299)',
                $config['parameters']['table']['tableName']
            )
        );
        $this->executeStatement(
            $this->connection,
            sprintf(
                'INSERT INTO %s ("name", "decimal") VALUES (\'beat\', 78.34567789)',
                $config['parameters']['table']['tableName']
            )
        );

        $newResult = $this->createApplication($config, $noNewRowsResult['state'])->run();
        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertEquals(4, $newResult['state']['lastFetchedRow']);
        $this->assertEquals(3, $newResult['imported']['rows']);
    }

    public function testIncrementalFetchingByDecimal(): void
    {
        $config = $this->getIncrementalFetchingConfig(self::DRIVER);
        $config['parameters']['incrementalFetchingColumn'] = 'decimal';
        $this->createIncrementalFetchingTable($config);

        $result = $this->createApplication($config)->run();

        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2,
            ],
            $result['imported']
        );

        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(38.9827423, $result['state']['lastFetchedRow']);

        $noNewRowsResult = $this->createApplication($config, $result['state'])->run();
        $this->assertEquals(1, $noNewRowsResult['imported']['rows']);
        $this->assertEquals($result['state'], $noNewRowsResult['state']);

        $this->executeStatement(
            $this->connection,
            sprintf(
                'INSERT INTO %s ("name", "decimal") VALUES (\'leo\', 50.89247299)',
                $config['parameters']['table']['tableName']
            )
        );
        $this->executeStatement(
            $this->connection,
            sprintf(
                'INSERT INTO %s ("name", "decimal") VALUES (\'beat\', 78.34567789)',
                $config['parameters']['table']['tableName']
            )
        );

        $newResult = $this->createApplication($config, $noNewRowsResult['state'])->run();
        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertEquals(78.34567789, $newResult['state']['lastFetchedRow']);
        $this->assertEquals(3, $newResult['imported']['rows']);
    }

    public function testIncrementalFetchingByTimestamp(): void
    {
        $config = $this->getIncrementalFetchingConfig(self::DRIVER);
        $config['parameters']['incrementalFetchingColumn'] = 'decimal';
        $this->createIncrementalFetchingTable($config);

        $result = $this->createApplication($config)->run();

        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2,
            ],
            $result['imported']
        );

        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertNotEmpty($result['state']['lastFetchedRow']);

        $noNewRowsResult = $this->createApplication($config, $result['state'])->run();
        $this->assertEquals(1, $noNewRowsResult['imported']['rows']);
        $this->assertEquals($result['state'], $noNewRowsResult['state']);

        $this->executeStatement(
            $this->connection,
            sprintf(
                'INSERT INTO %s ("name", "decimal") VALUES (\'leo\', 50.89247299)',
                $config['parameters']['table']['tableName']
            )
        );
        $this->executeStatement(
            $this->connection,
            sprintf(
                'INSERT INTO %s ("name", "decimal") VALUES (\'beat\', 78.34567789)',
                $config['parameters']['table']['tableName']
            )
        );

        $newResult = $this->createApplication($config, $noNewRowsResult['state'])->run();
        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertGreaterThan(
            $result['state']['lastFetchedRow'],
            $newResult['state']['lastFetchedRow']
        );
        $this->assertEquals(3, $newResult['imported']['rows']);
    }

    public function testIncrementalFetchingLimit(): void
    {
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingLimit'] = 1;
        $this->createIncrementalFetchingTable($config);
        for ($i = 0; $i < 10; $i++) {
            $this->executeStatement(
                $this->connection,
                sprintf(
                    'INSERT INTO %s ("name", "decimal") VALUES (\'%s\', %d)',
                    $config['parameters']['table']['tableName'],
                    $this->generateRandomString(),
                    mt_rand(0, 100)
                )
            );
        }
        $result = $this->createApplication($config)->run();
        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 1,
            ],
            $result['imported']
        );
        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(1, $result['state']['lastFetchedRow']);
        sleep(2);
        // since it's >= we'll set limit to 2 to fetch the second row also
        $config['parameters']['incrementalFetchingLimit'] = 2;
        $result = ($this->createApplication($config, $result['state']))->run();
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2,
            ],
            $result['imported']
        );
        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(2, $result['state']['lastFetchedRow']);
    }

    private function getIncrementalFetchingConfig(string $driver = self::DRIVER): array
    {
        $config = $this->getConfigRow($driver);
        unset($config['parameters']['query']);
        $config['parameters']['table'] = [
            'tableName' => 'AUTO_INCREMENT_TIMESTAMP',
            'schema' => 'TESTER',
        ];
        $config['parameters']['incremental'] = true;
        $config['parameters']['name'] = 'auto-increment-timestamp';
        $config['parameters']['outputTable'] = 'in.c-main.auto-increment-timestamp';
        $config['parameters']['primaryKey'] = ['id'];
        $config['parameters']['incrementalFetchingColumn'] = 'id';

        return $config;
    }

    private function generateRandomString(): string
    {
        $characters = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
        $randstring = '';
        for ($i = 0; $i < 10; $i++) {
            $randstring .= $characters[rand(0, strlen($characters))];
        }
        return $randstring;
    }
}
