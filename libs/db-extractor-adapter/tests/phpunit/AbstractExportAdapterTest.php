<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\Tests;

use Keboola\CommonExceptions\UserExceptionInterface;
use Keboola\DbExtractor\Adapter\ExportAdapter;
use Keboola\DbExtractor\Adapter\Query\SimpleQueryFactory;
use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Keboola\Temp\Temp;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\Constraint\StringContains;

abstract class AbstractExportAdapterTest extends BaseTest
{
    protected Temp $temp;

    abstract protected function createExportAdapter(
        array $state = [],
        ?string $host = null,
        ?int $port = null,
        ?SimpleQueryFactory $queryFactory = null
    ): ExportAdapter;

    protected function setUp(): void
    {
        parent::setUp();
        $this->temp = new Temp(self::class);
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->temp->remove();
    }

    public function testExportSimpleQuery(): void
    {
        $this->createTownsTable();
        $config = $this->createExportConfig([
            'table' => ['tableName' => 'towns', 'schema' => (string) getenv('DB_DATABASE')],
        ]);

        $result = $this->runExport($config);
        Assert::assertSame(6, $result->getRowsCount());
        Assert::assertSame(null, $result->getIncFetchingColMaxValue());

        // Without header, header is part of manifest for simple query
        $expectedCsv = <<<END
"1","Praha","1165581"
"2","Brno","369559"
"3","Ostrava","313088"
"4","Plzen","164180"
"5","Olomouc","101268"
"6","Liberec","97770"

END;
        Assert::assertSame($expectedCsv, file_get_contents($result->getCsvPath()));
    }

    public function testExportCustomQuery(): void
    {
        $this->createTownsTable();
        $config = $this->createExportConfig([
            'query' => 'SELECT * FROM `towns` ORDER BY `id` LIMIT 3',
        ]);

        $result = $this->runExport($config);
        Assert::assertSame(3, $result->getRowsCount());
        Assert::assertSame(null, $result->getIncFetchingColMaxValue());

        // With header, header is part of CSV for custom query
        $expectedCsv = <<<END
"id","name","population"
"1","Praha","1165581"
"2","Brno","369559"
"3","Ostrava","313088"

END;
        Assert::assertSame($expectedCsv, file_get_contents($result->getCsvPath()));
    }

    public function testExportFailed(): void
    {
        $this->createTownsTable();
        $retries = 4;
        $config = $this->createExportConfig([
            'table' => ['tableName' => 'towns', 'schema' => (string) getenv('DB_DATABASE')],
            'retries' => $retries,
        ]);

        $outputCsvFilePath = $this->getOutputCsvFilePath();
        $proxy = $this->createProxyToDb();
        $exportAdapter = $this->createExportAdapter([], self::TOXIPROXY_HOST, (int) $proxy->getListenPort());
        $this->makeProxyDown($proxy);

        try {
            $exportAdapter->export($config, $outputCsvFilePath);
            $this->fail('Exception expected');
        } catch (UserExceptionInterface $e) {
            Assert::assertStringContainsString('[output]: DB query failed:', $e->getMessage());
            Assert::assertStringContainsString("Tried $retries times.", $e->getMessage());
            Assert::assertThat($e->getMessage(), Assert::logicalOr(
                // Msg differs between PDO and ODBC
                new StringContains('Connection not open'),
                new StringContains('MySQL server has gone away'),
            ));
        }

        for ($attempt=1; $attempt < $retries; $attempt++) {
            Assert::assertTrue($this->logger->hasInfoThatContains("Retrying... [{$attempt}x]"));
        }
    }

    public function testExportIncrementalFetching(): void
    {
        $this->createTownsTable();
        $config = $this->createExportConfig([
            'table' => ['tableName' => 'towns', 'schema' => (string) getenv('DB_DATABASE')],
            'incrementalFetchingColumn' => 'id',
        ]);

        // First run
        $result = $this->runExport($config);
        Assert::assertSame(6, $result->getRowsCount());
        Assert::assertSame('6', $result->getIncFetchingColMaxValue());

        // Without header, header is part of manifest for simple query
        $expectedCsv = <<<END
"1","Praha","1165581"
"2","Brno","369559"
"3","Ostrava","313088"
"4","Plzen","164180"
"5","Olomouc","101268"
"6","Liberec","97770"

END;
        Assert::assertSame($expectedCsv, file_get_contents($result->getCsvPath()));

        // Second run
        $this->connection->query('INSERT INTO `towns` VALUES (7, "Ceske Budejovice", 96053)');
        $this->connection->query('INSERT INTO `towns` VALUES (8, "Hradec Kralove", 95195)');
        $result = $this->runExport($config, ['lastFetchedRow' => '6']);
        Assert::assertSame(3, $result->getRowsCount());
        Assert::assertSame('8', $result->getIncFetchingColMaxValue());

        // Last fetched row must be part of next export
        $expectedCsv = <<<END
"6","Liberec","97770"
"7","Ceske Budejovice","96053"
"8","Hradec Kralove","95195"

END;
        Assert::assertSame($expectedCsv, file_get_contents($result->getCsvPath()));
    }

    public function testExportIncrementalFetchingWithLimit(): void
    {
        $this->createTownsTable();
        $config = $this->createExportConfig([
            'table' => ['tableName' => 'towns', 'schema' => (string) getenv('DB_DATABASE')],
            'incrementalFetchingColumn' => 'id',
            'incrementalFetchingLimit' => 2,
        ]);

        // First run
        $result = $this->runExport($config);
        Assert::assertSame(2, $result->getRowsCount());
        Assert::assertSame('2', $result->getIncFetchingColMaxValue());

        $expectedCsv = <<<END
"1","Praha","1165581"
"2","Brno","369559"

END;
        Assert::assertSame($expectedCsv, file_get_contents($result->getCsvPath()));

        // Second run
        $result = $this->runExport($config, ['lastFetchedRow' => '2']);
        Assert::assertSame(2, $result->getRowsCount());
        Assert::assertSame('3', $result->getIncFetchingColMaxValue());

        // Last fetched row must be part of next export
        $expectedCsv = <<<END
"2","Brno","369559"
"3","Ostrava","313088"

END;
        Assert::assertSame($expectedCsv, file_get_contents($result->getCsvPath()));
    }

    private function createExportConfig(array $data): ExportConfig
    {
        $data['id'] = 123;
        $data['name'] = 'name';
        $data['outputTable'] = 'output';
        $data['retries'] = $data['retries'] ?? 3;
        $data['primaryKey'] = [];
        $data['query'] = $data['query'] ?? null;
        $data['columns'] = $data['columns'] ?? [];
        return ExportConfig::fromArray($data);
    }

    protected function runExport(ExportConfig $exportConfig, array $state = []): ExportResult
    {
        $outputCsvFilePath = $this->getOutputCsvFilePath();
        $exportAdapter = $this->createExportAdapter($state);
        return $exportAdapter->export($exportConfig, $outputCsvFilePath);
    }

    protected function getOutputCsvFilePath(): string
    {
        return $this->temp->getTmpFolder() . '/output.csv';
    }
}
