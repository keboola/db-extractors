<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration\NodeDefinition;

use Keboola\DbExtractorConfig\Configuration\NodeDefinition\DbNode;
use Symfony\Component\Config\Definition\Builder\NodeBuilder;

class MysqlDbNode extends DbNode
{
    protected function init(NodeBuilder $builder): void
    {
        parent::init($builder);
        $this->addNetworkCompression($builder);
    }

    protected function addNetworkCompression(NodeBuilder $builder): void
    {
        $builder->booleanNode('networkCompression')->defaultValue(false)->end();
    }
}
