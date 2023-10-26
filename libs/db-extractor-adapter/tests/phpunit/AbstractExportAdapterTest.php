<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\Tests;

use Keboola\CommonExceptions\UserExceptionInterface;
use Keboola\DbExtractor\Adapter\Exception\UserRetriedException;
use Keboola\DbExtractor\Adapter\ExportAdapter;
use Keboola\DbExtractor\Adapter\Query\QueryFactory;
use Keboola\DbExtractor\Adapter\ResultWriter\DefaultResultWriter;
use Keboola\DbExtractor\Adapter\ResultWriter\ResultWriter;
use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\Constraint\StringContains;

abstract class AbstractExportAdapterTest extends BaseTest
{
    abstract protected function createExportAdapter(
        array $state = [],
        ?string $host = null,
        ?int $port = null,
        ?QueryFactory $queryFactory = null,
    ): ExportAdapter;

    public function testExportSimpleQuery(): void
    {
        $this->createTownsTable();
        $config = $this->createExportConfig([
            'table' => ['tableName' => 'towns', 'schema' => (string) getenv('DB_DATABASE')],
        ]);

        $result = $this->runExport($config);
        Assert::assertSame(6, $result->getRowsCount());
        Assert::assertSame(null, $result->getIncFetchingColMaxValue());
        Assert::assertSame($this->temp->getTmpFolder() . '/out/tables/output.csv', $result->getCsvPath());

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
        Assert::assertSame($this->temp->getTmpFolder() . '/out/tables/output.csv', $result->getCsvPath());

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

        $proxy = $this->createProxyToDb();
        $exportAdapter = $this->createExportAdapter([], self::TOXIPROXY_HOST, (int) $proxy->getListenPort());
        $this->makeProxyDown($proxy);

        try {
            $exportAdapter->export($config, $this->getCsvFilePath());
            $this->fail('Exception expected');
        } catch (UserRetriedException $e) {
            Assert::assertStringContainsString('[output]: DB query failed:', $e->getMessage());
            Assert::assertStringContainsString("Tried $retries times.", $e->getMessage());
            Assert::assertSame($e->getTryCount(), $retries);
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
        Assert::assertSame($this->temp->getTmpFolder() . '/out/tables/output.csv', $result->getCsvPath());

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
        Assert::assertSame($this->temp->getTmpFolder() . '/out/tables/output.csv', $result->getCsvPath());

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
        Assert::assertSame($this->temp->getTmpFolder() . '/out/tables/output.csv', $result->getCsvPath());

        $expectedCsv = <<<END
"1","Praha","1165581"
"2","Brno","369559"

END;
        Assert::assertSame($expectedCsv, file_get_contents($result->getCsvPath()));

        // Second run
        $result = $this->runExport($config, ['lastFetchedRow' => '2']);
        Assert::assertSame(2, $result->getRowsCount());
        Assert::assertSame('3', $result->getIncFetchingColMaxValue());
        Assert::assertSame($this->temp->getTmpFolder() . '/out/tables/output.csv', $result->getCsvPath());

        // Last fetched row must be part of next export
        $expectedCsv = <<<END
"2","Brno","369559"
"3","Ostrava","313088"

END;
        Assert::assertSame($expectedCsv, file_get_contents($result->getCsvPath()));
    }

    protected function runExport(ExportConfig $exportConfig, array $state = []): ExportResult
    {
        return $this->createExportAdapter($state)->export($exportConfig, $this->getCsvFilePath());
    }

    protected function createResultWriter(array $state): ResultWriter
    {
        return new DefaultResultWriter($state);
    }
}
