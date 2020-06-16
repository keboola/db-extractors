<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Configuration\NodeDefinition;

use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\NodeDefinition;
use Symfony\Component\Config\Definition\Builder\NodeParentInterface;

class DbNode extends ArrayNodeDefinition
{
    public const NODE_NAME = 'db';

    protected NodeDefinition $sshNode;

    protected SslNode $sslNode;

    public function __construct(
        ?SshNode $sshNode = null,
        ?SslNode $sslNode = null,
        ?NodeParentInterface $parent = null
    ) {
        parent::__construct(self::NODE_NAME, $parent);
        $this->sshNode = $sshNode ?? new SshNode();
        $this->sslNode = $sslNode ?? new SslNode();
        $this->init();
    }

    protected function init(): void
    {
        // @formatter:off
        $this
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
                ->append($this->sshNode)
                ->append($this->sslNode)
            ->end()
        ;
        // @formatter:on
    }
}
