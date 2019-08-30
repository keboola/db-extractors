<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Configuration;

use Keboola\Component\Config\BaseConfigDefinition;
use Keboola\DbExtractorConfig\Configuration\NodeDefinition\DbNode;
use Keboola\DbExtractorConfig\Configuration\NodeDefinition\NodeDefinitionInterface;
use Keboola\DbExtractorConfig\Configuration\NodeDefinition\TablesNode;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\NodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class ConfigDefinition extends BaseConfigDefinition
{
    /** @var NodeDefinition */
    protected $dbNodeDefinition;

    /** @var NodeDefinition */
    protected $tablesNodeDefinition;

    public function __construct(
        ?NodeDefinitionInterface $dbNode = null,
        ?NodeDefinitionInterface $sshNode = null,
        ?NodeDefinitionInterface $tablesNode = null
    ) {
        if (is_null($dbNode)) {
            $dbNode = new DbNode($sshNode);
        }
        if (is_null($tablesNode)) {
            $tablesNode = new TablesNode();
        }
        $this->dbNodeDefinition = $dbNode->create();
        $this->tablesNodeDefinition = $tablesNode->create();
    }

    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $treeBuilder = new TreeBuilder();

        /** @var ArrayNodeDefinition $parametersNode */
        $parametersNode = $treeBuilder->root('parameters');

        // @formatter:off
        $parametersNode
            ->children()
                ->scalarNode('data_dir')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('extractor_class')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->append($this->dbNodeDefinition)
                ->append($this->tablesNodeDefinition)
            ->end();
        // @formatter:on
        return $parametersNode;
    }
}
