<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Configuration\NodeDefinition;

use Symfony\Component\Config\Definition\Builder\NodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class DbNode implements NodeDefinitionInterface
{
    /** @var NodeDefinition */
    private $sshNode;

    public function __construct(?NodeDefinitionInterface $sshNode = null)
    {
        if (is_null($sshNode)) {
            $sshNode = new SshNode();
        }

        $this->sshNode = $sshNode->create();
    }

    public function create(): NodeDefinition
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
                ->append($this->sshNode)
            ->end()
        ;
        // @formatter:on

        return $node;
    }
}
