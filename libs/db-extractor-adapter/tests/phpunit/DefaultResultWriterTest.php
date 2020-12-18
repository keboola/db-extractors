<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\Tests;

use ArrayIterator;
use Keboola\DbExtractor\Adapter\ResultWriter\DefaultResultWriter;
use Keboola\DbExtractor\Adapter\ResultWriter\ResultWriter;
use Keboola\DbExtractor\Adapter\ValueObject\QueryResult;
use PHPUnit\Framework\Assert;

class DefaultResultWriterTest extends BaseTest
{
    public function testSimpleQuery(): void
    {
        $exportConfig = $this->createExportConfig([
            'table' => ['tableName' => 'foo', 'schema' => 'bar'],
        ]);

        $queryResult = $this->createQueryResultMock([
            ['id' => 1, 'name' => 'Praha', 'population' => 1165581],
            ['id' => 2, 'name' => 'Brno', 'population' => 369559],
            ['id' => 3, 'name' => 'Ostrava', 'population' => 313088],
        ]);

        $result = $this
            ->createWriter()
            ->writeToCsv($queryResult, $exportConfig, $this->getCsvFilePath());

        Assert::assertSame(3, $result->getRowsCount());
        Assert::assertSame(null, $result->getIncFetchingColMaxValue());
        Assert::assertSame($this->temp->getTmpFolder() . '/out/tables/output.csv', $result->getCsvPath());

        // Header is part of manifest when is simple query used (table metadata are available).
        $expectedCsv = <<<END
"1","Praha","1165581"
"2","Brno","369559"
"3","Ostrava","313088"

END;
        Assert::assertSame($expectedCsv, file_get_contents($result->getCsvPath()));
    }

    public function testSimpleQueryNoRows(): void
    {
        $exportConfig = $this->createExportConfig([
            'table' => ['tableName' => 'foo', 'schema' => 'bar'],
        ]);

        $queryResult = $this->createQueryResultMock([]);

        $result = $this
            ->createWriter()
            ->writeToCsv($queryResult, $exportConfig, $this->getCsvFilePath());

        Assert::assertSame(0, $result->getRowsCount());
        Assert::assertSame(null, $result->getIncFetchingColMaxValue());
        Assert::assertSame($this->temp->getTmpFolder() . '/out/tables/output.csv', $result->getCsvPath());

        // No rows, no CSV file
        Assert::assertFalse(file_exists($result->getCsvPath()));
    }

    public function testCustomQuery(): void
    {
        $exportConfig = $this->createExportConfig([
            'query' => 'SELECT * FROM `foo`',
        ]);

        $queryResult = $this->createQueryResultMock([
            ['id' => 1, 'name' => 'Praha', 'population' => 1165581],
            ['id' => 2, 'name' => 'Brno', 'population' => 369559],
            ['id' => 3, 'name' => 'Ostrava', 'population' => 313088],
        ]);

        $result = $this
            ->createWriter()
            ->writeToCsv($queryResult, $exportConfig, $this->getCsvFilePath());

        Assert::assertSame(3, $result->getRowsCount());
        Assert::assertSame(null, $result->getIncFetchingColMaxValue());
        Assert::assertSame($this->temp->getTmpFolder() . '/out/tables/output.csv', $result->getCsvPath());

        // Header is part of manifest when is custom query used (table metadata are NOT available).
        $expectedCsv = <<<END
"id","name","population"
"1","Praha","1165581"
"2","Brno","369559"
"3","Ostrava","313088"

END;
        Assert::assertSame($expectedCsv, file_get_contents($result->getCsvPath()));
    }

    public function testCustomQueryNoRows(): void
    {
        $exportConfig = $this->createExportConfig([
            'query' => 'SELECT * FROM `foo`',
        ]);

        $queryResult = $this->createQueryResultMock([]);

        $result = $this
            ->createWriter()
            ->writeToCsv($queryResult, $exportConfig, $this->getCsvFilePath());

        Assert::assertSame(0, $result->getRowsCount());
        Assert::assertSame(null, $result->getIncFetchingColMaxValue());
        Assert::assertSame($this->temp->getTmpFolder() . '/out/tables/output.csv', $result->getCsvPath());

        // No rows, no CSV file
        Assert::assertFalse(file_exists($result->getCsvPath()));
    }

    public function testIncrementalFetching(): void
    {
        $exportConfig = $this->createExportConfig([
            'table' => ['tableName' => 'foo', 'schema' => 'bar'],
            'incrementalFetchingColumn' => 'id',
        ]);

        $queryResult = $this->createQueryResultMock([
            ['id' => 3, 'name' => 'Ostrava', 'population' => 313088],
            ['id' => 4, 'name' => 'Plzen', 'population' => 164180],
            ['id' => 5, 'name' => 'Olomouc', 'population' => 101268],
        ]);

        $result = $this
            ->createWriter([]) // no state
            ->writeToCsv($queryResult, $exportConfig, $this->getCsvFilePath());

        Assert::assertSame(3, $result->getRowsCount());
        Assert::assertSame('5', $result->getIncFetchingColMaxValue());
        Assert::assertSame($this->temp->getTmpFolder() . '/out/tables/output.csv', $result->getCsvPath());

        $expectedCsv = <<<END
"3","Ostrava","313088"
"4","Plzen","164180"
"5","Olomouc","101268"

END;
        Assert::assertSame($expectedCsv, file_get_contents($result->getCsvPath()));
    }

    public function testIncrementalFetchingState(): void
    {
        $exportConfig = $this->createExportConfig([
            'table' => ['tableName' => 'foo', 'schema' => 'bar'],
            'incrementalFetchingColumn' => 'id',
        ]);

        $queryResult = $this->createQueryResultMock([
            ['id' => 3, 'name' => 'Ostrava', 'population' => 313088],
            ['id' => 4, 'name' => 'Plzen', 'population' => 164180],
            ['id' => 5, 'name' => 'Olomouc', 'population' => 101268],
        ]);

        $result = $this
            ->createWriter(['lastFetchedRow' => '3'])
            ->writeToCsv($queryResult, $exportConfig, $this->getCsvFilePath());

        Assert::assertSame(3, $result->getRowsCount());
        Assert::assertSame('5', $result->getIncFetchingColMaxValue());
        Assert::assertSame($this->temp->getTmpFolder() . '/out/tables/output.csv', $result->getCsvPath());

        $expectedCsv = <<<END
"3","Ostrava","313088"
"4","Plzen","164180"
"5","Olomouc","101268"

END;
        Assert::assertSame($expectedCsv, file_get_contents($result->getCsvPath()));
    }

    public function testIncrementalFetchingPreserveStateWhenNoRows(): void
    {
        $exportConfig = $this->createExportConfig([
            'table' => ['tableName' => 'foo', 'schema' => 'bar'],
            'incrementalFetchingColumn' => 'id',
        ]);

        $incFetchingLastValue = '3';

        $queryResult = $this->createQueryResultMock([]);

        $result = $this
            ->createWriter(['lastFetchedRow' => $incFetchingLastValue])
            ->writeToCsv($queryResult, $exportConfig, $this->getCsvFilePath());

        Assert::assertSame(0, $result->getRowsCount());
        Assert::assertSame($incFetchingLastValue, $result->getIncFetchingColMaxValue());
        Assert::assertSame($this->temp->getTmpFolder() . '/out/tables/output.csv', $result->getCsvPath());

        // No rows, no CSV file
        Assert::assertFalse(file_exists($result->getCsvPath()));
    }

    private function createQueryResultMock(array $rows): QueryResult
    {
        $queryResult = $this
            ->getMockBuilder(QueryResult::class)
            ->disableAutoReturnValueGeneration()
            ->getMock();
        $queryResult
            ->expects($this->once())
            ->id('getIteratorCall')
            ->method('getIterator')
            ->willReturn(new ArrayIterator($rows));
        $queryResult
            ->expects($this->once())
            ->after('getIteratorCall')
            ->method('closeCursor');
        return $queryResult;
    }

    private function createWriter(array $state = []): ResultWriter
    {
        return new DefaultResultWriter($state);
    }
}
