<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration\NodeDefinition;

use Keboola\DbExtractorConfig\Configuration\NodeDefinition\DbNode;
use Keboola\DbExtractorConfig\Configuration\NodeDefinition\SshNode;
use Symfony\Component\Config\Definition\Builder\NodeDefinition;
use Symfony\Component\Config\Definition\Builder\NodeParentInterface;

class MysqlDbNode extends DbNode
{
    /** @var NodeDefinition */
    protected $sslNode;

    public function __construct(?SshNode $sshNode = null, ?NodeParentInterface $parent = null)
    {
        $this->sslNode = new MysqlSslNode();

        parent::__construct($sshNode, $parent);
    }

    protected function init(): void
    {
        // @formatter:off
        $this
            ->children()
                ->scalarNode('driver')->end()
                ->scalarNode('host')->end()
                ->scalarNode('port')->end()
                ->scalarNode('database')->end()
                ->scalarNode('user')
                    ->isRequired()
                ->end()
                ->scalarNode('#password')
                    ->isRequired()
                ->end()
                ->append($this->sshNode)
                ->append($this->sslNode)
                ->booleanNode('networkCompression')->defaultValue(false)->end()
            ->end()
        ;
        // @formatter:on
    }
}
