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
            ->scalarNode('query')
                ->cannotBeEmpty()
            ->end()
            ->arrayNode('table')
                ->children()
                    ->scalarNode('schema')
                        ->cannotBeEmpty()
                        ->isRequired()
                    ->end()
                    ->scalarNode('tableName')
                        ->cannotBeEmpty()
                        ->isRequired()
                    ->end()
                ->end()
            ->end()
            ->arrayNode('columns')
                ->prototype('scalar')
                    ->cannotBeEmpty()
                ->end()
            ->end()
            ->scalarNode('outputTable')
                ->isRequired()
                ->cannotBeEmpty()
            ->end()
            ->booleanNode('incremental')
                ->defaultValue(false)
            ->end()
            ->scalarNode('incrementalFetchingColumn')
                ->cannotBeEmpty()
            ->end()
            ->integerNode('incrementalFetchingLimit')
                ->min(0)
            ->end()
            ->booleanNode('enabled')
                ->defaultValue(true)
            ->end()
            ->arrayNode('primaryKey')
                ->prototype('scalar')
                    ->cannotBeEmpty()
                ->end()
            ->end()
            ->integerNode('retries')
                ->min(0)
            ->end();
    }

    public function validate(array $v): array
    {
        if (empty($v['query']) && empty($v['table'])) {
            throw new InvalidConfigurationException('Table or query must be configured.');
        }

        if (!empty($v['query']) && !empty($v['table'])) {
            throw new InvalidConfigurationException('Both table and query cannot be set together.');
        }

        if (!empty($v['query'])  && !empty($v['incrementalFetchingColumn'])) {
            throw new InvalidConfigurationException(
                'The "incrementalFetchingColumn" is configured, ' .
                'but incremental fetching is not supported for custom query.'
            );
        }

        if (!empty($v['query'])  && !empty($v['incrementalFetchingLimit'])) {
            throw new InvalidConfigurationException(
                'The "incrementalFetchingLimit" is configured, ' .
                'but incremental fetching is not supported for custom query.'
            );
        }

        if ($v['incremental'] === true && empty($v['incrementalFetchingColumn'])) {
            throw new InvalidConfigurationException(
                'The "incrementalFetchingColumn" must be configured, if is incremental fetching enabled.'
            );
        }

        if ($v['incremental'] === false && !empty($v['incrementalFetchingColumn'])) {
            throw new InvalidConfigurationException(
                'The "incrementalFetchingColumn" is configured, ' .
                'but incremental fetching is not enabled.'
            );
        }

        if ($v['incremental'] === false && !empty($v['incrementalFetchingLimit'])) {
            throw new InvalidConfigurationException(
                'The "incrementalFetchingLimit" is configured, ' .
                'but incremental fetching is not enabled.'
            );
        }

        return $v;
    }
}
