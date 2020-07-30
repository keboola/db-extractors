<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Configuration\NodeDefinition;

use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\NodeBuilder;
use Symfony\Component\Config\Definition\Builder\NodeParentInterface;

class SslNode extends ArrayNodeDefinition
{
    private const NODE_NAME = 'ssl';

    public function __construct(?NodeParentInterface $parent = null)
    {
        parent::__construct(self::NODE_NAME, $parent);
        $this->init($this->children());
    }

    public function init(NodeBuilder $nodeBuilder): void
    {
        $this->addEnabledNode($nodeBuilder);
        $this->addKeyNode($nodeBuilder);
        $this->addCaNode($nodeBuilder);
        $this->addCertNode($nodeBuilder);
        $this->addCipherNode($nodeBuilder);
        $this->addVerifyServerCertNode($nodeBuilder);
        $this->addIgnoreCertificateCn($nodeBuilder);
    }

    protected function addEnabledNode(NodeBuilder $nodeBuilder): void
    {
        $nodeBuilder->booleanNode('enabled')->defaultFalse();
    }

    protected function addKeyNode(NodeBuilder $nodeBuilder): void
    {
        $nodeBuilder->scalarNode('key');
    }

    protected function addCaNode(NodeBuilder $nodeBuilder): void
    {
        $nodeBuilder->scalarNode('ca');
    }

    protected function addCertNode(NodeBuilder $nodeBuilder): void
    {
        $nodeBuilder->scalarNode('cert');
    }

    protected function addCipherNode(NodeBuilder $nodeBuilder): void
    {
        $nodeBuilder->scalarNode('cipher');
    }

    protected function addVerifyServerCertNode(NodeBuilder $nodeBuilder): void
    {
        $nodeBuilder->scalarNode('verifyServerCert');
    }

    protected function addIgnoreCertificateCn(NodeBuilder $nodeBuilder): void
    {
        $nodeBuilder->scalarNode('ignoreCertificateCn');
    }
}
