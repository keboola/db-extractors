<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Configuration;

use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\NodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\ConfigurationInterface;

abstract class AbstractConfigDefinition implements ConfigurationInterface
{

    /**
     * @return ArrayNodeDefinition|NodeDefinition
     */
    public function addSshNode()
    {
        $builder = new TreeBuilder();
        $node = $builder->root('ssh');

        // @formatter:off
        $node
            ->children()
                ->booleanNode('enabled')->end()
                ->arrayNode('keys')
                    ->children()
                        ->scalarNode('private')->end()
                        ->scalarNode('#private')->end()
                        ->scalarNode('public')->end()
                    ->end()
                ->end()
                ->scalarNode('sshHost')->end()
                ->scalarNode('sshPort')->end()
                ->scalarNode('remoteHost')->end()
                ->scalarNode('remotePort')->end()
                ->scalarNode('localPort')->end()
                ->scalarNode('user')->end()
                ->booleanNode('compression')
                    ->defaultValue(false)
                ->end()
            ->end();
        // @formatter:on
        return $node;
    }

    protected function addValidation(NodeDefinition $definition): NodeDefinition
    {
        $definition
            ->validate()
            ->ifTrue(function ($v) {
                if (isset($v['query']) && $v['query'] !== '' && isset($v['table'])) {
                    return true;
                }
                return false;
            })
            ->thenInvalid('Both table and query cannot be set together.')
            ->end()
            ->validate()
            ->ifTrue(function ($v) {
                if (isset($v['query']) && $v['query'] !== '' && isset($v['incrementalFetchingColumn'])) {
                    return true;
                }
                return false;
            })->thenInvalid('Incremental fetching is not supported for advanced queries.')
            ->end()
            ->validate()
            ->ifTrue(function ($v) {
                if (!isset($v['table']) && !isset($v['query'])) {
                    return true;
                }
                return false;
            })->thenInvalid('One of table or query is required')
            ->end()
        ;

        return $definition;
    }

    public function addDbNode(): NodeDefinition
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
                ->append($this->addSshNode())
            ->end();
        // @formatter:on

        return $node;
    }
}
