<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration\NodeDefinition;

use Keboola\DbExtractorConfig\Configuration\NodeDefinition\TableNodesDecorator;
use Symfony\Component\Config\Definition\Builder\NodeBuilder;

class MysqlTableNodesDecorator extends TableNodesDecorator
{
    public function addNodes(NodeBuilder $builder): void
    {
        parent::addNodes($builder);

        //Backwards compatibility with old configurations. Not used
        $builder->booleanNode('advancedMode')->end();
    }
}
