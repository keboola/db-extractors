<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Tests\ValueObject;

use Keboola\DbExtractorConfig\Configuration\ValueObject\IncrementalFetchingConfig;
use Keboola\DbExtractorConfig\Exception\PropertyNotSetException;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

class IncrementalFetchingConfigTest extends TestCase
{
    public function testEmpty(): void
    {
        Assert::assertSame(null, IncrementalFetchingConfig::fromArray([]));
    }

    public function testNotEnabled(): void
    {
        Assert::assertSame(null, IncrementalFetchingConfig::fromArray(['incremental' => false]));
    }

    public function testColumn(): void
    {
        /** @var IncrementalFetchingConfig $config */
        $config = IncrementalFetchingConfig::fromArray([
            'incremental' => true,
            'incrementalFetchingColumn' => 'col123',
        ]);
        Assert::assertSame('col123', $config->getColumn());
        Assert::assertSame(false, $config->hasLimit());
        try {
            $config->getLimit();
        } catch (PropertyNotSetException $e) {
            // ok
        }
    }

    public function testColumnAndLimit(): void
    {
        /** @var IncrementalFetchingConfig $config */
        $config = IncrementalFetchingConfig::fromArray([
            'incremental' => true,
            'incrementalFetchingColumn' => 'col123',
            'incrementalFetchingLimit' => 456,
        ]);
        Assert::assertSame('col123', $config->getColumn());
        Assert::assertSame(true, $config->hasLimit());
        Assert::assertSame(456, $config->getLimit());
    }
}
