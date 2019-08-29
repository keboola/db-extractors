<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Configuration\NodeDefinition;

use Symfony\Component\Config\Definition\Builder\NodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class DbNode implements NodeDefinitionInterface
{
    public static function create(): NodeDefinition
    {
        $builder = new TreeBuilder();
        $node = $builder->root('db');

        // @formatter:off
        $node
            ->children()
                ->scalarNode('driver')->end()
                ->scalarNode('host')->end()
                ->scalarNode('port')->end()
                ->scalarNode('database')
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('user')
                    ->isRequired()
                ->end()
                ->scalarNode('#password')
                    ->isRequired()
                ->end()
                ->append(SshNode::create())
            ->end()
        ;
        // @formatter:on

        return $node;
    }
}
