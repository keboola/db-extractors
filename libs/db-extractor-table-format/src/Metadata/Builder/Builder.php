<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TableResultFormat\Metadata\Builder;

use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject;

interface Builder
{
    public static function create(): self;

    public function build(): ValueObject;
}
