<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Configuration\NodeDefinition;

use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\NodeDefinition;
use Symfony\Component\Config\Definition\Builder\NodeParentInterface;

class DbNode extends ArrayNodeDefinition
{
    public const NODE_NAME = 'db';

    /** @var NodeDefinition */
    protected $sshNode;

    public function __construct(?SshNode $sshNode = null, ?NodeParentInterface $parent = null)
    {
        parent::__construct(self::NODE_NAME, $parent);

        if (is_null($sshNode)) {
            $sshNode = new SshNode();
        }
        $this->sshNode = $sshNode;

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
            ->end()
        ;
        // @formatter:on
    }
}
