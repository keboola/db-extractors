<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Configuration\NodeDefinition;

use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\NodeBuilder;
use Symfony\Component\Config\Definition\Builder\NodeParentInterface;

class SshNode extends ArrayNodeDefinition
{
    public const NODE_NAME = 'ssh';

    public function __construct(?NodeParentInterface $parent = null)
    {
        parent::__construct(self::NODE_NAME, $parent);
        $this->init($this->children());
    }

    protected function init(NodeBuilder $builder): void
    {
        $this->addEnabledNode($builder);
        $this->addKeysNode($builder);
        $this->addSshHostNode($builder);
        $this->addSshPortNode($builder);
        $this->addRemoteHostNode($builder);
        $this->addRemotePortNode($builder);
        $this->addLocalPortNode($builder);
        $this->addUserNode($builder);
        $this->addCompressionNode($builder);
        $this->addMaxRetriesNode($builder);
    }

    protected function addEnabledNode(NodeBuilder $builder): void
    {
        $builder->booleanNode('enabled');
    }

    protected function addKeysNode(NodeBuilder $builder): void
    {
        // @formatter:off
        $builder->arrayNode('keys')
            ->children()
                ->scalarNode('#private')->end()
                ->scalarNode('public')->end();
        // @formatter:on
    }

    protected function addSshHostNode(NodeBuilder $builder): void
    {
        $builder->scalarNode('sshHost');
    }

    protected function addSshPortNode(NodeBuilder $builder): void
    {
        $builder->scalarNode('sshPort');
    }

    protected function addRemoteHostNode(NodeBuilder $builder): void
    {
        $builder->scalarNode('remoteHost');
    }

    protected function addRemotePortNode(NodeBuilder $builder): void
    {
        $builder->scalarNode('remotePort');
    }

    protected function addLocalPortNode(NodeBuilder $builder): void
    {
        $builder->scalarNode('localPort');
    }

    protected function addUserNode(NodeBuilder $builder): void
    {
        $builder->scalarNode('user');
    }

    protected function addCompressionNode(NodeBuilder $builder): void
    {
        $builder->booleanNode('compression')->defaultValue(false);
    }

    protected function addMaxRetriesNode(NodeBuilder $builder): void
    {
        $builder->scalarNode('maxRetries');
    }
}
