<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration\NodeDefinition;

use Keboola\DbExtractorConfig\Configuration\NodeDefinition\DbNode;
use Symfony\Component\Config\Definition\Builder\NodeBuilder;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class OracleDbNode extends DbNode
{
    protected function init(NodeBuilder $builder): void
    {
        parent::init($builder);
        $this->addTnsnamesNode($builder);
        $this->addConnectThrough($builder);
        $this->addDefaultRowPrefetch($builder);

        $this->validate()->always(function ($v) {
            if (empty($v['host']) && empty($v['tnsnames'])) {
                throw new InvalidConfigurationException('Host or tnsnames must be configured.');
            }

            if (!empty($v['tnsnames']) && (!empty($v['host']) || !empty($v['port']))) {
                throw new InvalidConfigurationException('Tnsnames and host/port cannot be set together.');
            }

            return $v;
        });
    }

    protected function addTnsnamesNode(NodeBuilder $builder): void
    {
        $builder->scalarNode('tnsnames');
    }

    protected function addConnectThrough(NodeBuilder $builder): void
    {
        $builder->booleanNode('connectThrough')->defaultFalse();
    }

    protected function addDefaultRowPrefetch(NodeBuilder $builder): void
    {
        $builder->scalarNode('defaultRowPrefetch');
    }

    protected function addSslNode(NodeBuilder $builder): void
    {
        // not implemented
    }
}
