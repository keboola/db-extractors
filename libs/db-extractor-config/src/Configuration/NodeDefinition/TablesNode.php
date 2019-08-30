<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Configuration\NodeDefinition;

use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\NodeParentInterface;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class TablesNode extends ArrayNodeDefinition
{
    public const NODE_NAME = 'tables';

    public function __construct(?NodeParentInterface $parent = null)
    {
        parent::__construct(self::NODE_NAME, $parent);

        $this->init();
    }

    protected function init(): void
    {
        // @formatter:off
        $this
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
    }
}
