<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Configuration;

use Keboola\Component\Config\BaseConfigDefinition;
use Keboola\DbExtractorConfig\Configuration\NodeDefinition\DbNode;
use Keboola\DbExtractorConfig\Configuration\NodeDefinition\TablesNode;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class ConfigDefinition extends BaseConfigDefinition
{
    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $treeBuilder = new TreeBuilder();
        $parametersNode = $treeBuilder->root('parameters');
        // @formatter:off
        $parametersNode
            ->children()
                ->scalarNode('data_dir')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('extractor_class')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->append(DbNode::create())
                ->append(TablesNode::create())
            ->end();
        // @formatter:on
        return $parametersNode;
    }
}
