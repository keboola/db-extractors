<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Configuration\NodeDefinition;

use Symfony\Component\Config\Definition\Builder\NodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class TablesNode implements NodeDefinitionInterface
{
    public function create(): NodeDefinition
    {
        $builder = new TreeBuilder();
        $node = $builder->root('tables');

        // @formatter:off
        $node
            ->prototype('array')
            ->validate()->always(function ($v) {
                if (isset($v['query']) && $v['query'] !== '' && isset($v['table'])) {
                    throw new InvalidConfigurationException('Both table and query cannot be set together.');
                }
                if (isset($v['query']) && $v['query'] !== '' && isset($v['incrementalFetchingColumn'])) {
                    $message = 'Incremental fetching is not supported for advanced queries.';
                    throw new InvalidConfigurationException($message);
                }
                if (!isset($v['table']) && !isset($v['query'])) {
                    throw new InvalidConfigurationException('One of table or query is required');
                }
                return $v;
            })->end()
            ->ignoreExtraKeys(true)
            ->children()
                ->integerNode('id')
                    ->isRequired()
                    ->min(0)
                ->end()
                ->scalarNode('name')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('query')->end()
                ->arrayNode('table')
                    ->children()
                        ->scalarNode('schema')->end()
                        ->scalarNode('tableName')->end()
                    ->end()
                ->end()
                ->arrayNode('columns')
                    ->prototype('scalar')->end()
                ->end()
                ->scalarNode('outputTable')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->booleanNode('incremental')
                    ->defaultValue(false)
                ->end()
                ->booleanNode('enabled')
                    ->defaultValue(true)
                ->end()
                ->arrayNode('primaryKey')
                    ->prototype('scalar')->end()
                ->end()
                ->integerNode('retries')
                    ->min(0)
                ->end()
            ->end()
        ;
        // @formatter:on

        return $node;
    }
}
