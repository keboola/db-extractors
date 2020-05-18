<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Configuration;

use Keboola\Component\Config\BaseConfigDefinition;
use Keboola\DbExtractorConfig\Configuration\NodeDefinition\DbNode;
use Keboola\DbExtractorConfig\Configuration\NodeDefinition\SshNode;
use Keboola\DbExtractorConfig\Configuration\NodeDefinition\TablesNode;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\NodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class ConfigDefinition extends BaseConfigDefinition
{
    protected NodeDefinition $dbNode;

    protected NodeDefinition $tablesNode;

    public function __construct(
        ?DbNode $dbNode = null,
        ?SshNode $sshNode = null,
        ?TablesNode $tablesNode = null
    ) {
        $this->dbNode = $dbNode ?? new DbNode($sshNode);
        $this->tablesNode = $tablesNode ?? new TablesNode();
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
                ->append($this->tablesNode)
            ->end();
        // @formatter:on
        return $parametersNode;
    }
}
