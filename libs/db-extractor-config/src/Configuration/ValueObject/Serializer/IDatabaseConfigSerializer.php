<?php

declare(strict_types=1);


namespace Keboola\DbExtractorConfig\Configuration\ValueObject\Serializer;


use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;

interface IDatabaseConfigSerializer
{
    public static function serialize(DatabaseConfig $databaseConfig): array;
}
