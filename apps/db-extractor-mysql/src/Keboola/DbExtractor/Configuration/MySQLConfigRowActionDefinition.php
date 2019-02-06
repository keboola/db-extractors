<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

use Symfony\Component\Config\Definition\Builder\NodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class MySQLConfigRowActionDefinition extends ConfigRowDefinition
{
    public function getConfigTreeBuilder(): TreeBuilder
    {
        $treeBuilder = new TreeBuilder();
        $rootNode = $treeBuilder->root('parameters');

        // @formatter:off
        $rootNode
            ->children()
                ->scalarNode('data_dir')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('extractor_class')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->arrayNode('db')
                    ->children()
                        ->scalarNode('driver')->end()
                        ->scalarNode('host')
                            ->isRequired()
                        ->end()
                        ->scalarNode('port')->end()
                        ->scalarNode('database')->end()
                        ->scalarNode('user')
                            ->isRequired()
                        ->end()
                        ->scalarNode('#password')
                            ->isRequired()
                        ->end()
                        ->append($this->addSshNode())
                        ->append($this->addSslNode())
                        ->booleanNode('networkCompression')
                        ->defaultValue(false)
                        ->end()
                    ->end()
                ->end()
            ->end()
        ;
        // @formatter:on
        return $treeBuilder;
    }

    public function addSslNode(): NodeDefinition
    {
        $builder = new TreeBuilder();
        $node = $builder->root('ssl');

        // @formatter:off
        $node
            ->children()
                ->booleanNode('enabled')->end()
                ->scalarNode('ca')->end()
                ->scalarNode('cert')->end()
                ->scalarNode('key')->end()
                ->scalarNode('cipher')->end()
                ->booleanNode('verifyServerCert')->defaultTrue()->end()
            ->end();
        // @formatter:on
        return $node;
    }
}
