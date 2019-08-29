<?php



namespace Keboola\DbExtractorConfig\Configuration\NodeDefinition;

use Symfony\Component\Config\Definition\Builder\NodeDefinition;

interface NodeDefinitionInterface
{
    public static function create(): NodeDefinition;
}
