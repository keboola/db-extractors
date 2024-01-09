<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Adapter\ValueObject\QueryMetadata;
use Keboola\DbExtractor\Manifest\DefaultManifestGenerator;

class OracleManifestGenerator extends DefaultManifestGenerator
{
    protected function generateColumnsFromQueryMetadata(array &$manifestData, QueryMetadata $queryMetadata): void
    {
        // @TODO finish getColumns in table exporter for custom query
    }
}
