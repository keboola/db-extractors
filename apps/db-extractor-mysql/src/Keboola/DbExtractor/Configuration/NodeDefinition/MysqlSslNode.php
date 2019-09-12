<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration\NodeDefinition;

use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\NodeDefinition;
use Symfony\Component\Config\Definition\Builder\NodeParentInterface;

class MysqlSslNode extends ArrayNodeDefinition
{
    public const NODE_NAME = 'ssl';

    public function __construct(?NodeParentInterface $parent = null)
    {
        parent::__construct(self::NODE_NAME, $parent);

        $this->init();
    }

    protected function init(): void
    {
        // @formatter:off
        $this
            ->children()
                ->booleanNode('enabled')->end()
                ->scalarNode('ca')->end()
                ->scalarNode('cert')->end()
                ->scalarNode('key')->end()
                ->scalarNode('cipher')->end()
                ->booleanNode('verifyServerCert')->defaultTrue()->end()
            ->end()
        ;
        // @formatter:on
    }
}
