<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration\NodeDefinition;

use Keboola\DbExtractorConfig\Configuration\NodeDefinition\SslNode;
use Symfony\Component\Config\Definition\Builder\NodeBuilder;

class MysqlSslNode extends SslNode
{

    public function init(NodeBuilder $nodeBuilder): void
    {
        parent::init($nodeBuilder);

        // @formatter:off
        $this
            ->validate()
                ->ifTrue(function ($v) {
                    // either both or none must be specified
                    return isset($v['cert']) xor isset($v['#key']);
                })
                ->thenInvalid('Both "#key" and "cert" must be specified')
            ->end()
        ;
        // @formatter:on
    }
}
