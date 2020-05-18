<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Tests\ValueObject;

use Keboola\DbExtractorConfig\Configuration\ValueObject\InputTable;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

class InputTableTest extends TestCase
{
    public function testValid(): void
    {
        $table = InputTable::fromArray([
            'table' => [
                'tableName' => 'table123',
                'schema' => 'schema456',
            ],
        ]);

        Assert::assertSame('table123', $table->getName());
        Assert::assertSame('schema456', $table->getSchema());
    }
}
