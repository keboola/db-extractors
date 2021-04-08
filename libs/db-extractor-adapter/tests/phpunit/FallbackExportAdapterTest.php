<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\Tests;

use Keboola\DbExtractor\Adapter\Exception\InvalidArgumentException;
use Keboola\DbExtractor\Adapter\FallbackExportAdapter;
use Keboola\DbExtractor\Adapter\Tests\Fixtures\FailingExportAdapter;
use Keboola\DbExtractor\Adapter\Tests\Fixtures\FailingExportAdapterException;
use Keboola\DbExtractor\Adapter\Tests\Fixtures\PassingExportAdapter;
use Keboola\DbExtractor\Adapter\Tests\Fixtures\SkippedExportAdapter;
use Keboola\DbExtractor\Adapter\Tests\Fixtures\SkippedExportAdapterException;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use PHPUnit\Framework\Assert;

class FallbackExportAdapterTest extends BaseTest
{
    public function testNoAdapter(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('At least one adapter must be specified.');
        new FallbackExportAdapter($this->logger, []);
    }

    public function testPassing1(): void
    {
        $adapter1 = new PassingExportAdapter('Adapter1');
        $fallbackAdapter = new FallbackExportAdapter($this->logger, [
            $adapter1,
        ]);

        $result = $fallbackAdapter->export($this->createDummyExportConfig(), '/some/path/output.csv');
        Assert::assertSame(0, $result->getRowsCount());
        Assert::assertSame(1, $adapter1->getExportCallCount());
        Assert::assertTrue($this->logger->hasInfoThatContains('Exporting by "Adapter1" adapter.'));
    }

    public function testPassing2(): void
    {
        $adapter1 = new PassingExportAdapter('Adapter1');
        $adapter2 = new PassingExportAdapter('Adapter2');
        $adapter3 = new PassingExportAdapter('Adapter3');
        $fallbackAdapter = new FallbackExportAdapter($this->logger, [
            $adapter1,
            $adapter2,
            $adapter3,
        ]);

        $result = $fallbackAdapter->export($this->createDummyExportConfig(), '/some/path/output.csv');
        Assert::assertSame(0, $result->getRowsCount());
        Assert::assertSame(1, $adapter1->getExportCallCount());
        Assert::assertSame(0, $adapter2->getExportCallCount());
        Assert::assertSame(0, $adapter3->getExportCallCount());
        Assert::assertTrue($this->logger->hasInfoThatContains('Exporting by "Adapter1" adapter.'));
    }

    public function testPassing3(): void
    {
        $adapter1 = new PassingExportAdapter('Adapter1');
        $adapter2 = new FailingExportAdapter('Adapter2');
        $adapter3 = new FailingExportAdapter('Adapter3');
        $fallbackAdapter = new FallbackExportAdapter($this->logger, [
            $adapter1,
            $adapter2,
            $adapter3,
        ]);

        $result = $fallbackAdapter->export($this->createDummyExportConfig(), '/some/path/output.csv');
        Assert::assertSame(0, $result->getRowsCount());
        Assert::assertSame(1, $adapter1->getExportCallCount());
        Assert::assertSame(0, $adapter2->getExportCallCount());
        Assert::assertSame(0, $adapter3->getExportCallCount());
        Assert::assertTrue($this->logger->hasInfoThatContains('Exporting by "Adapter1" adapter.'));
    }

    public function testFailing1(): void
    {
        $adapter1 = new FailingExportAdapter('Adapter1');
        $fallbackAdapter = new FallbackExportAdapter($this->logger, [
            $adapter1,
        ]);

        try {
            $fallbackAdapter->export($this->createDummyExportConfig(), '/some/path/output.csv');
            Assert::fail('Exception expected.');
        } catch (FailingExportAdapterException $e) {
            Assert::assertSame('Something went wrong.', $e->getMessage());
        }

        Assert::assertSame(1, $adapter1->getExportCallCount());
        Assert::assertTrue($this->logger->hasInfoThatContains(
            'Exporting by "Adapter1" adapter.'
        ));
        Assert::assertTrue($this->logger->hasWarningThatContains(
            'Export by "Adapter1" adapter failed: Something went wrong.'
        ));
    }

    public function testFailing2(): void
    {
        $adapter1 = new FailingExportAdapter('Adapter1');
        $adapter2 = new FailingExportAdapter('Adapter2');
        $adapter3 = new FailingExportAdapter('Adapter3');
        $fallbackAdapter = new FallbackExportAdapter($this->logger, [
            $adapter1,
            $adapter2,
            $adapter3,
        ]);

        try {
            $fallbackAdapter->export($this->createDummyExportConfig(), '/some/path/output.csv');
            Assert::fail('Exception expected.');
        } catch (FailingExportAdapterException $e) {
            Assert::assertSame('Something went wrong.', $e->getMessage());
        }

        Assert::assertSame(1, $adapter1->getExportCallCount());
        Assert::assertSame(1, $adapter2->getExportCallCount());
        Assert::assertSame(1, $adapter3->getExportCallCount());
        Assert::assertTrue($this->logger->hasInfoThatContains(
            'Exporting by "Adapter1" adapter.'
        ));
        Assert::assertTrue($this->logger->hasWarningThatContains(
            'Export by "Adapter1" adapter failed: Something went wrong.'
        ));
        Assert::assertTrue($this->logger->hasInfoThatContains(
            'Exporting by "Adapter2" adapter.'
        ));
        Assert::assertTrue($this->logger->hasWarningThatContains(
            'Export by "Adapter2" adapter failed: Something went wrong.'
        ));
        Assert::assertTrue($this->logger->hasInfoThatContains(
            'Exporting by "Adapter3" adapter.'
        ));
        Assert::assertTrue($this->logger->hasWarningThatContains(
            'Export by "Adapter3" adapter failed: Something went wrong.'
        ));
    }

    public function testFallback(): void
    {
        $adapter1 = new FailingExportAdapter('Adapter1');
        $adapter2 = new PassingExportAdapter('Adapter2');
        $adapter3 = new FailingExportAdapter('Adapter3');
        $fallbackAdapter = new FallbackExportAdapter($this->logger, [
            $adapter1,
            $adapter2,
            $adapter3,
        ]);

        $result = $fallbackAdapter->export($this->createDummyExportConfig(), 'output.csv');

        Assert::assertSame(0, $result->getRowsCount());
        Assert::assertSame(1, $adapter1->getExportCallCount());
        Assert::assertSame(1, $adapter2->getExportCallCount());
        Assert::assertSame(0, $adapter3->getExportCallCount());
        Assert::assertTrue($this->logger->hasInfoThatContains(
            'Exporting by "Adapter1" adapter.'
        ));
        Assert::assertTrue($this->logger->hasWarningThatContains(
            'Export by "Adapter1" adapter failed: Something went wrong.'
        ));
        Assert::assertTrue($this->logger->hasInfoThatContains(
            'Exporting by "Adapter2" adapter.'
        ));
    }

    public function testSkipped(): void
    {
        $adapter1 = new SkippedExportAdapter('Adapter1');
        $adapter2 = new PassingExportAdapter('Adapter2');
        $fallbackAdapter = new FallbackExportAdapter($this->logger, [
            $adapter1,
            $adapter2,
        ]);

        $result = $fallbackAdapter->export($this->createDummyExportConfig(), 'output.csv');

        Assert::assertSame(0, $result->getRowsCount());
        Assert::assertSame(1, $adapter1->getExportCallCount());
        Assert::assertSame(1, $adapter2->getExportCallCount());
        Assert::assertTrue($this->logger->hasInfoThatContains(
            'Exporting by "Adapter1" adapter.'
        ));
        Assert::assertTrue($this->logger->hasInfoThatContains(
            'Adapter "Adapter1" skipped: skipped message'
        ));
        Assert::assertTrue($this->logger->hasInfoThatContains(
            'Exporting by "Adapter2" adapter.'
        ));
    }

    private function createDummyExportConfig(): ExportConfig
    {
        return $this->createExportConfig([
            'query' => 'SELECT * FROM foo',
        ]);
    }
}
