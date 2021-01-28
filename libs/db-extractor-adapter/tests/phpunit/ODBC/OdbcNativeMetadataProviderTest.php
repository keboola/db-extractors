<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\Tests\ODBC;

use Keboola\DbExtractor\Adapter\ODBC\OdbcNativeMetadataProvider;
use Keboola\DbExtractor\Adapter\Tests\BaseTest;
use Keboola\DbExtractor\Adapter\Tests\Traits\OdbcCreateConnectionTrait;
use Keboola\DbExtractorConfig\Configuration\ValueObject\InputTable;
use PHPUnit\Framework\Assert;

class OdbcNativeMetadataProviderTest extends BaseTest
{
    use OdbcCreateConnectionTrait;

    private OdbcNativeMetadataProvider $metadataProvider;

    protected function setUp(): void
    {
        parent::setUp();
        $this->metadataProvider = new OdbcNativeMetadataProvider($this->createOdbcConnection(), $this->getDatabase());
        $this->dropAllTables();
        $this->createTownsTable();
        $this->createProductsTable();
        $this->createProductsView();
    }

    public function testAllTablesWithColumns(): void
    {
        $tables = $this->metadataProvider->listTables()->getAll();
        Assert::assertCount(3, $tables);
        $table1 = $tables[0];
        $table1Cols = $table1->getColumns()->getAll();
        $table2 = $tables[1];
        $table2Cols = $table2->getColumns()->getAll();
        $table3 = $tables[2];
        $table3Cols = $table3->getColumns()->getAll();

        // TABLE 1
        Assert::assertSame('products', $table1->getName());
        Assert::assertSame($this->getDatabase(), $table1->getCatalog());
        Assert::assertSame('table', $table1->getType());
        Assert::assertCount(4, $table1Cols);
        Assert::assertSame(1, $table1Cols[0]->getOrdinalPosition());
        // ----
        Assert::assertSame('id', $table1Cols[0]->getName());
        Assert::assertSame('INT', $table1Cols[0]->getType());
        Assert::assertFalse($table1Cols[0]->hasLength());
        Assert::assertSame(true, $table1Cols[0]->isNullable());
        Assert::assertSame(null, $table1Cols[0]->getDefault());
        // ----
        Assert::assertSame(2, $table1Cols[1]->getOrdinalPosition());
        Assert::assertSame('name', $table1Cols[1]->getName());
        Assert::assertSame('VARCHAR', $table1Cols[1]->getType());
        Assert::assertSame('255', $table1Cols[1]->getLength());
        Assert::assertSame(false, $table1Cols[1]->isNullable());
        Assert::assertSame(null, $table1Cols[1]->getDefault());
        // ----
        Assert::assertSame(3, $table1Cols[2]->getOrdinalPosition());
        Assert::assertSame('price', $table1Cols[2]->getName());
        Assert::assertSame('DECIMAL', $table1Cols[2]->getType());
        Assert::assertSame('20,10', $table1Cols[2]->getLength());
        Assert::assertSame(false, $table1Cols[2]->isNullable());
        Assert::assertSame(null, $table1Cols[2]->getDefault());
        // ----
        Assert::assertSame(4, $table1Cols[3]->getOrdinalPosition());
        Assert::assertSame('in_stock', $table1Cols[3]->getName());
        Assert::assertSame('SMALLINT', $table1Cols[3]->getType());
        Assert::assertFalse($table1Cols[3]->hasLength());
        Assert::assertSame(false, $table1Cols[3]->isNullable());
        Assert::assertSame('1', $table1Cols[3]->getDefault());

        // TABLE 2 (VIEW)
        Assert::assertSame('products_view', $table2->getName());
        Assert::assertSame($this->getDatabase(), $table2->getCatalog());
        Assert::assertSame('view', $table2->getType());
        Assert::assertCount(4, $table2Cols);
        // ----
        Assert::assertSame('id', $table2Cols[0]->getName());
        Assert::assertSame('INT', $table2Cols[0]->getType());
        Assert::assertFalse($table2Cols[0]->hasLength());
        Assert::assertSame(false, $table2Cols[0]->isNullable());
        Assert::assertSame('0', $table2Cols[0]->getDefault());
        // ----
        Assert::assertSame(2, $table2Cols[1]->getOrdinalPosition());
        Assert::assertSame('name', $table2Cols[1]->getName());
        Assert::assertSame('VARCHAR', $table2Cols[1]->getType());
        Assert::assertSame('255', $table2Cols[1]->getLength());
        Assert::assertSame(false, $table2Cols[1]->isNullable());
        Assert::assertSame(null, $table2Cols[1]->getDefault());
        // ----
        Assert::assertSame(3, $table2Cols[2]->getOrdinalPosition());
        Assert::assertSame('price', $table2Cols[2]->getName());
        Assert::assertSame('DECIMAL', $table2Cols[2]->getType());
        Assert::assertSame('20,10', $table2Cols[2]->getLength());
        Assert::assertSame(false, $table2Cols[2]->isNullable());
        Assert::assertSame(null, $table2Cols[2]->getDefault());
        // ----
        Assert::assertSame(4, $table2Cols[3]->getOrdinalPosition());
        Assert::assertSame('in_stock', $table2Cols[3]->getName());
        Assert::assertSame('SMALLINT', $table2Cols[3]->getType());
        Assert::assertFalse($table2Cols[3]->hasLength());
        Assert::assertSame(false, $table2Cols[3]->isNullable());
        Assert::assertSame('1', $table2Cols[3]->getDefault());

        // TABLE 3
        Assert::assertSame('towns', $table3->getName());
        Assert::assertSame($this->getDatabase(), $table3->getCatalog());
        Assert::assertSame('table', $table3->getType());
        Assert::assertCount(3, $table3Cols);
        // ----
        Assert::assertSame('id', $table3Cols[0]->getName());
        Assert::assertSame('INT', $table3Cols[0]->getType());
        Assert::assertFalse($table3Cols[0]->hasLength());
        Assert::assertSame(true, $table3Cols[0]->isNullable());
        Assert::assertSame(null, $table3Cols[0]->getDefault());
        // ----
        Assert::assertSame(2, $table3Cols[1]->getOrdinalPosition());
        Assert::assertSame('name', $table3Cols[1]->getName());
        Assert::assertSame('VARCHAR', $table3Cols[1]->getType());
        Assert::assertSame('255', $table3Cols[1]->getLength());
        Assert::assertSame(false, $table3Cols[1]->isNullable());
        Assert::assertSame(null, $table3Cols[1]->getDefault());
        // ----
        Assert::assertSame(3, $table3Cols[2]->getOrdinalPosition());
        Assert::assertSame('population', $table3Cols[2]->getName());
        Assert::assertSame('INT', $table3Cols[2]->getType());
        Assert::assertFalse($table3Cols[2]->hasLength());
        Assert::assertSame(false, $table3Cols[2]->isNullable());
        Assert::assertSame(null, $table3Cols[2]->getDefault());
    }

    public function testAllTablesWithoutColumns(): void
    {
        $tables = $this->metadataProvider->listTables([], false)->getAll();
        Assert::assertCount(3, $tables);
        $table1 = $tables[0];
        Assert::assertSame('products', $table1->getName());
        Assert::assertSame($this->getDatabase(), $table1->getCatalog());
        Assert::assertSame('table', $table1->getType());
        Assert::assertFalse($table1->hasColumns());
        $table2 = $tables[1];
        Assert::assertSame('products_view', $table2->getName());
        Assert::assertSame($this->getDatabase(), $table2->getCatalog());
        Assert::assertSame('view', $table2->getType());
        Assert::assertFalse($table2->hasColumns());
        $table3 = $tables[2];
        Assert::assertFalse($table3->hasColumns());
        Assert::assertSame('towns', $table3->getName());
        Assert::assertSame($this->getDatabase(), $table3->getCatalog());
        Assert::assertSame('table', $table3->getType());
    }

    public function testWhitelistWithColumns(): void
    {
        $whitelist = [
            new InputTable('towns', $this->getDatabase()),
            new InputTable('products_view', $this->getDatabase()),
        ];
        $tables = $this->metadataProvider->listTables($whitelist)->getAll();
        Assert::assertCount(2, $tables);
        $table1 = $tables[0];
        $table1Cols = $table1->getColumns()->getAll();
        $table2 = $tables[1];
        $table2Cols = $table2->getColumns()->getAll();

        Assert::assertSame('products_view', $table1->getName());
        Assert::assertSame($this->getDatabase(), $table1->getCatalog());
        Assert::assertSame('view', $table1->getType());
        Assert::assertCount(4, $table1Cols);

        Assert::assertSame('towns', $table2->getName());
        Assert::assertSame($this->getDatabase(), $table2->getCatalog());
        Assert::assertSame('table', $table2->getType());
        Assert::assertCount(3, $table2Cols);
    }

    public function testWhitelistWithoutColumns(): void
    {
        $whitelist = [
            new InputTable('towns', $this->getDatabase()),
            new InputTable('products_view', $this->getDatabase()),
        ];
        $tables = $this->metadataProvider->listTables($whitelist, false)->getAll();
        Assert::assertCount(2, $tables);
        $table1 = $tables[0];
        $table2 = $tables[1];

        Assert::assertSame('products_view', $table1->getName());
        Assert::assertSame($this->getDatabase(), $table1->getCatalog());
        Assert::assertSame('view', $table1->getType());
        Assert::assertFalse($table1->hasColumns());

        Assert::assertSame('towns', $table2->getName());
        Assert::assertSame($this->getDatabase(), $table2->getCatalog());
        Assert::assertSame('table', $table2->getType());
        Assert::assertFalse($table2->hasColumns());
    }

    public function testOnlyForCatalog(): void
    {
        $this->metadataProvider = new OdbcNativeMetadataProvider($this->createOdbcConnection(), 'abc');
        Assert::assertCount(0, $this->metadataProvider->listTables()->getAll());
    }
}
