<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Configuration;

use Keboola\Component\Config\BaseConfigDefinition;
use Keboola\DbExtractorConfig\Configuration\NodeDefinition\DbNode;
use Keboola\DbExtractorConfig\Configuration\NodeDefinition\SshNode;
use Keboola\DbExtractorConfig\Configuration\NodeDefinition\SslNodeDecorator;
use Keboola\DbExtractorConfig\Configuration\NodeDefinition\TableNodesDecorator;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\NodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class ConfigDefinition extends BaseConfigDefinition
{
    protected NodeDefinition $dbNode;

    protected TableNodesDecorator $tableNodesDecorator;

    public function __construct(
        ?DbNode $dbNode = null,
        ?SshNode $sshNode = null,
        ?SslNodeDecorator $sslNodeDecorator = null,
        ?TableNodesDecorator $tableNodesDecorator = null
    ) {
        $this->dbNode = $dbNode ?? new DbNode($sshNode, $sslNodeDecorator);
        $this->tableNodesDecorator = $tableNodesDecorator ?? new TableNodesDecorator();
    }

    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $treeBuilder = new TreeBuilder('parameters');

        /** @var ArrayNodeDefinition $parametersNode */
        $parametersNode = $treeBuilder->getRootNode();

        // @formatter:off
        $parametersNode
            ->ignoreExtraKeys(false)
            ->children()
                ->scalarNode('data_dir')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('extractor_class')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->append($this->dbNode)
            ->end();
        // @formatter:on

        // Add common nodes for tables/rows config
        $tablesItemNode = $parametersNode->children()->arrayNode('tables')->prototype('array');
        $this->tableNodesDecorator->addNodes($tablesItemNode->children());
        $tablesItemNode->validate()->always(fn($v) => $this->tableNodesDecorator->validate($v));

        return $parametersNode;
    }
}
