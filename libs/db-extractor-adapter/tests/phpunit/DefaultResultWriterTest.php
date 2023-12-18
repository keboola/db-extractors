<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\Tests;

use ArrayIterator;
use Keboola\DbExtractor\Adapter\ResultWriter\DefaultResultWriter;
use Keboola\DbExtractor\Adapter\ValueObject\QueryMetadata;
use Keboola\DbExtractor\Adapter\ValueObject\QueryResult;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\ColumnBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\ColumnCollection;
use PHPUnit\Framework\Assert;

class DefaultResultWriterTest extends BaseTest
{
    public function testSimpleQuery(): void
    {
        $exportConfig = $this->createExportConfig([
            'table' => ['tableName' => 'foo', 'schema' => 'bar'],
        ]);

        $queryResult = $this->createQueryResultMock(
            ['id' => 'integer', 'name' => 'string'],
            [
                ['id' => 1, 'name' => 'Praha', 'population' => 1165581],
                ['id' => 2, 'name' => 'Brno', 'population' => 369559],
                ['id' => 3, 'name' => 'Ostrava', 'population' => 313088],
            ],
        );

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

        $queryResult = $this->createQueryResultMock(['id' => 'integer', 'name' => 'string'], []);

        $result = $this
            ->createWriter()
            ->writeToCsv($queryResult, $exportConfig, $this->getCsvFilePath());

        Assert::assertSame(0, $result->getRowsCount());
        Assert::assertSame(null, $result->getIncFetchingColMaxValue());
        Assert::assertSame($this->temp->getTmpFolder() . '/out/tables/output.csv', $result->getCsvPath());

        // No rows, but CSV file must exists
        Assert::assertTrue(file_exists($result->getCsvPath()));
        $expectedCsv = '';
        Assert::assertSame($expectedCsv, file_get_contents($result->getCsvPath()));
    }

    public function testCustomQuery(): void
    {
        $exportConfig = $this->createExportConfig([
            'query' => 'SELECT * FROM `foo`',
        ]);

        $queryResult = $this->createQueryResultMock(
            ['id' => 'integer', 'name' => 'string', 'population' => 'integer'],
            [
                ['id' => 1, 'name' => 'Praha', 'population' => 1165581],
                ['id' => 2, 'name' => 'Brno', 'population' => 369559],
                ['id' => 3, 'name' => 'Ostrava', 'population' => 313088],
            ],
        );

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

        $queryResult = $this->createQueryResultMock(['id' => 'integer', 'name' => 'string'], []);

        $result = $this
            ->createWriter()
            ->writeToCsv($queryResult, $exportConfig, $this->getCsvFilePath());

        Assert::assertSame(0, $result->getRowsCount());
        Assert::assertSame(null, $result->getIncFetchingColMaxValue());
        Assert::assertSame($this->temp->getTmpFolder() . '/out/tables/output.csv', $result->getCsvPath());

        // No rows, but CSV file must exists
        Assert::assertTrue(file_exists($result->getCsvPath()));
        $expectedCsv = "\"id\",\"name\"\n";
        Assert::assertSame($expectedCsv, file_get_contents($result->getCsvPath()));
    }

    public function testIncrementalFetching(): void
    {
        $exportConfig = $this->createExportConfig([
            'table' => ['tableName' => 'foo', 'schema' => 'bar'],
            'incrementalFetchingColumn' => 'id',
        ]);

        $queryResult = $this->createQueryResultMock(
            ['id' => 'integer', 'name' => 'string'],
            [
                ['id' => 3, 'name' => 'Ostrava', 'population' => 313088],
                ['id' => 4, 'name' => 'Plzen', 'population' => 164180],
                ['id' => 5, 'name' => 'Olomouc', 'population' => 101268],
            ],
        );

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

        $queryResult = $this->createQueryResultMock(
            ['id' => 'integer', 'name' => 'string'],
            [
                ['id' => 3, 'name' => 'Ostrava', 'population' => 313088],
                ['id' => 4, 'name' => 'Plzen', 'population' => 164180],
                ['id' => 5, 'name' => 'Olomouc', 'population' => 101268],
            ],
        );

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

        $queryResult = $this->createQueryResultMock(['id' => 'integer', 'name' => 'string'], []);

        $result = $this
            ->createWriter(['lastFetchedRow' => $incFetchingLastValue])
            ->writeToCsv($queryResult, $exportConfig, $this->getCsvFilePath());

        Assert::assertSame(0, $result->getRowsCount());
        Assert::assertSame($incFetchingLastValue, $result->getIncFetchingColMaxValue());
        Assert::assertSame($this->temp->getTmpFolder() . '/out/tables/output.csv', $result->getCsvPath());

        // No rows, but CSV file must exists
        Assert::assertTrue(file_exists($result->getCsvPath()));
        $expectedCsv = '';
        Assert::assertSame($expectedCsv, file_get_contents($result->getCsvPath()));
    }

    public function testNotIgnoreBadUtf8ByDefault(): void
    {
        $exportConfig = $this->createExportConfig([
            'table' => ['tableName' => 'foo', 'schema' => 'bar'],
        ]);

        $queryResult = $this->createQueryResultMock(
            ['id' => 'integer', 'name' => 'string'],
            [
                ['id' => 1, 'name' => "Ost\xa0\xa1rava", 'population' => 313088],
                ['id' => 2, 'name' => "Plzen\xa0\xa1", 'population' => 164180],
                ['id' => 3, 'name' => "\xa0\xa1Olomouc", 'population' => 101268],
            ],
        );

        $result = $this
            ->createWriter([])
            ->writeToCsv($queryResult, $exportConfig, $this->getCsvFilePath());

        $expectedCsv = <<<END
"1","Ost\xa0\xa1rava","313088"
"2","Plzen\xa0\xa1","164180"
"3","\xa0\xa1Olomouc","101268"

END;
        Assert::assertSame($expectedCsv, file_get_contents($result->getCsvPath()));
    }

    public function testIgnoreBadUtf8(): void
    {
        $exportConfig = $this->createExportConfig([
            'table' => ['tableName' => 'foo', 'schema' => 'bar'],
        ]);

        $queryResult = $this->createQueryResultMock(
            ['id' => 'integer', 'name' => 'string'],
            [
                ['id' => 1, 'name' => "Ost\xa0\xa1rava", 'population' => 313088],
                ['id' => 2, 'name' => "Plzen\xa0\xa1", 'population' => 164180],
                ['id' => 3, 'name' => "\xa0\xa1Olomouc", 'population' => 101268],
                ['id' => 4, 'name' => "世界\xE2\x82", 'population' => 103001256],
            ],
        );

        $result = $this
            ->createWriter([])
            ->setIgnoreInvalidUtf8() // <<<<<<<<<<<<<<<<<<<<
            ->writeToCsv($queryResult, $exportConfig, $this->getCsvFilePath());

        $expectedCsv = <<<END
"1","Ostrava","313088"
"2","Plzen","164180"
"3","Olomouc","101268"
"4","世界","103001256"

END;
        Assert::assertSame($expectedCsv, file_get_contents($result->getCsvPath()));
    }

    public function testConvertEncoding(): void
    {
        $exportConfig = $this->createExportConfig([
            'table' => ['tableName' => 'foo', 'schema' => 'bar'],
        ]);

        $queryResult = $this->createQueryResultMock(
            ['id' => 'integer', 'name' => 'string'],
            [
                ['id' => 1, 'name' => 'ABCÁÈÕ', 'population' => 313088],
                ['id' => 2, 'name' => 'CDEÁÈÕ', 'population' => 164180],
            ],
        );

        $result = $this
            ->createWriter([])
            ->setConvertEncoding('UTF-8', 'ASCII//TRANSLIT') // <<<<<<<<<<<<<<<<<<<<
            ->writeToCsv($queryResult, $exportConfig, $this->getCsvFilePath());

        $expectedCsv = <<<END
"1","ABCAEO","313088"
"2","CDEAEO","164180"

END;
        Assert::assertSame($expectedCsv, file_get_contents($result->getCsvPath()));
    }

    /**
     * @dataProvider getUtf8Data
     */
    public function testConvertEncodingInArray(array $data, array $expected): void
    {
        $fixed = DefaultResultWriter::convertEncodingInArray($data, 'UTF-8', 'UTF-8//IGNORE');
        Assert::assertSame($expected, $fixed);
    }

    public function getUtf8Data(): iterable
    {
        yield 'numbers' => [
            [1, 2, 3],
            ['1', '2', '3'],
        ];

        yield 'strings' => [
            ['abc', 'def', 'xyz'],
            ['abc', 'def', 'xyz'],
        ];

        yield 'invalid-utf-8' => [
            ["aa\xa0\xa1bb", "\xa0\xa1"],
            ['aabb', ''],
        ];
    }

    private function createQueryResultMock(array $columns, array $rows): QueryResult
    {
        $columnsMetadata = [];
        foreach ($columns as $name => $type) {
            $builder = ColumnBuilder::create();
            $builder->setName($name);
            $builder->setType($type);
            $columnsMetadata[] = $builder->build();
        }
        $metadata  = $this
            ->getMockBuilder(QueryMetadata::class)
            ->disableAutoReturnValueGeneration()
            ->getMock();
        $metadata
            ->expects($this->any())
            ->method('getColumns')
            ->willReturn(new ColumnCollection($columnsMetadata));

        $queryResult = $this
            ->getMockBuilder(QueryResult::class)
            ->disableAutoReturnValueGeneration()
            ->getMock();
        $queryResult
            ->expects($this->any())
            ->method('getMetadata')
            ->willReturn($metadata);
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

    private function createWriter(array $state = []): DefaultResultWriter
    {
        return new DefaultResultWriter($state);
    }
}
