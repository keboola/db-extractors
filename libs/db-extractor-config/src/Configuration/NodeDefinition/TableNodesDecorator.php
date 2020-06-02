<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Configuration\NodeDefinition;

use Symfony\Component\Config\Definition\Builder\NodeBuilder;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class TableNodesDecorator
{
    public const DEFAULT_MAX_TRIES = 5;

    public function normalize(array $v): array
    {
        // Backward compatibility: some older configurations may use "zero" in the meaning of "disabled"".
        if (($v['incrementalFetchingLimit'] ?? null) === 0) {
            $v['incrementalFetchingLimit'] = null;
        }

        return $v;
    }

    public function validate(array $v): array
    {
        $v  = $this->normalize($v);

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
                'The "incrementalFetchingColumn" must be configured, if incremental fetching is enabled.'
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

    public function addNodes(NodeBuilder $builder): void
    {
        $this->addIdNode($builder);
        $this->addNameNode($builder);
        $this->addQueryNode($builder);
        $this->addTableNode($builder);
        $this->addColumnsNode($builder);
        $this->addOutputTableNode($builder);
        $this->addIncrementalFetchingNodes($builder);
        $this->addEnabledNode($builder);
        $this->addPrimaryKeyNode($builder);
        $this->addRetriesNode($builder);
    }

    protected function addIdNode(NodeBuilder $builder): void
    {
        $builder->integerNode('id')->min(0);
    }

    protected function addNameNode(NodeBuilder $builder): void
    {
        $builder->scalarNode('name')->cannotBeEmpty();
    }

    protected function addQueryNode(NodeBuilder $builder): void
    {
        // @formatter:off
        $builder
            ->scalarNode('query')
                ->defaultNull()
                ->cannotBeEmpty();
        // @formatter:on
    }

    protected function addTableNode(NodeBuilder $builder): void
    {
        // @formatter:off
        $builder
            ->arrayNode('table')
                ->children()
                    ->scalarNode('schema')
                        ->cannotBeEmpty()
                        ->isRequired()
                    ->end()
                    ->scalarNode('tableName')
                        ->cannotBeEmpty()
                        ->isRequired()
                    ->end();
        // @formatter:on
    }

    protected function addColumnsNode(NodeBuilder $builder): void
    {
        // @formatter:off
        $builder
            ->arrayNode('columns')
                ->prototype('scalar')
                    ->cannotBeEmpty();
        // @formatter:on
    }

    protected function addOutputTableNode(NodeBuilder $builder): void
    {
        // @formatter:off
        $builder
            ->scalarNode('outputTable')
                ->isRequired()
                ->cannotBeEmpty();
        // @formatter:on
    }

    protected function addIncrementalFetchingNodes(NodeBuilder $builder): void
    {
        // @formatter:off
        $builder
            ->booleanNode('incremental')
                ->defaultValue(false)
            ->end()
            ->scalarNode('incrementalFetchingColumn')
                ->cannotBeEmpty()
            ->end()
            ->integerNode('incrementalFetchingLimit')
                ->min(0) // zero is taken as disabled
            ->end();
        // @formatter:on
    }

    protected function addEnabledNode(NodeBuilder $builder): void
    {
        $builder->booleanNode('enabled')->defaultValue(true);
    }

    protected function addPrimaryKeyNode(NodeBuilder $builder): void
    {
        // @formatter:off
        $builder
            ->arrayNode('primaryKey')
                ->prototype('scalar')
                    ->cannotBeEmpty()
                ->end();
        // @formatter:on
    }

    protected function addRetriesNode(NodeBuilder $builder): void
    {
        // @formatter:off
        $builder
            ->integerNode('retries')
                ->min(0)
                ->defaultValue(self::DEFAULT_MAX_TRIES);
        // @formatter:on
    }
}
