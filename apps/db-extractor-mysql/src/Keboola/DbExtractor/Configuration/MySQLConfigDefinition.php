<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

use Symfony\Component\Config\Definition\Builder\NodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class MySQLConfigDefinition extends ConfigDefinition
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
                        ->scalarNode('host')->end()
                        ->scalarNode('port')->end()
                        ->scalarNode('database')->end()
                        ->scalarNode('user')->isRequired()->end()
                        ->scalarNode('password')->end()
                        ->scalarNode('#password')->end()
                        ->append($this->addSshNode())
                        ->append($this->addSslNode())
                        ->booleanNode('networkCompression')->defaultValue(false)->end()
                    ->end()
                ->end()
                ->arrayNode('tables')
                    ->prototype('array')
                        ->children()
                            ->integerNode('id')->isRequired()->min(0)->end()
                            ->scalarNode('name')->isRequired()->cannotBeEmpty()->end()
                            ->arrayNode('table')
                                ->children()
                                    ->scalarNode('schema')->end()
                                    ->scalarNode('tableName')->end()
                                ->end()
                            ->end()
                            ->arrayNode('columns')
                                ->prototype('scalar')->end()
                            ->end()
                            ->scalarNode('query')->end()
                            ->scalarNode('outputTable')->isRequired()->cannotBeEmpty()->end()
                            ->booleanNode('incremental')->defaultValue(false)->end()
                            ->booleanNode('enabled')->defaultValue(true)->end()
                            ->arrayNode('primaryKey')
                                ->prototype('scalar')->end()
                            ->end()
                            ->integerNode('retries')->min(1)->end()
                            ->booleanNode('advancedMode')->end()
                        ->end()
                    ->end()
                ->end()
            ->end();
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
            ->end();
        // @formatter:on
        return $node;
    }
}
