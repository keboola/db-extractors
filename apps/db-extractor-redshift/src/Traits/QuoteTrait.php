<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Traits;

trait QuoteTrait
{
    public function quoteIdentifier(string $value): string
    {
        $q = '"';
        return ($q . str_replace("$q", "$q$q", $value) . $q);
    }
}
