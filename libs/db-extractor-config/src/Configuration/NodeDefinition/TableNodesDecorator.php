<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Configuration\NodeDefinition;

use Symfony\Component\Config\Definition\Builder\NodeBuilder;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class TableNodesDecorator
{
    public function addNodes(NodeBuilder $builder): void
    {
        $builder
            ->integerNode('id')
                ->min(0)
            ->end()
            ->scalarNode('name')
                ->cannotBeEmpty()
            ->end()
            ->scalarNode('query')->end()
            ->arrayNode('table')
                ->children()
                    ->scalarNode('schema')->isRequired()->end()
                    ->scalarNode('tableName')->isRequired()->end()
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
            ->scalarNode('incrementalFetchingColumn')->end()
            ->scalarNode('incrementalFetchingLimit')->end()
            ->booleanNode('enabled')
                ->defaultValue(true)
            ->end()
            ->arrayNode('primaryKey')
                ->prototype('scalar')->end()
            ->end()
            ->integerNode('retries')
                ->min(0)
            ->end();
    }

    public function validate(array $v): array
    {
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
    }
}
