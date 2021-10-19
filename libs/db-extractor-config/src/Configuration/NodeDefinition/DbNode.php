<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Configuration\NodeDefinition;

use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\NodeBuilder;
use Symfony\Component\Config\Definition\Builder\NodeDefinition;
use Symfony\Component\Config\Definition\Builder\NodeParentInterface;

class DbNode extends ArrayNodeDefinition
{
    public const NODE_NAME = 'db';

    protected NodeDefinition $sshNode;

    protected SslNode $sslNode;

    public function __construct(
        ?SshNode $sshNode = null,
        ?SslNode $sslNode = null,
        ?NodeParentInterface $parent = null
    ) {
        parent::__construct(self::NODE_NAME, $parent);
        $this->sshNode = $sshNode ?? new SshNode();
        $this->sslNode = $sslNode ?? new SslNode();
        $this->init($this->children());
    }

    protected function init(NodeBuilder $builder): void
    {
        $this->addDriverNode($builder);
        $this->addHostNode($builder);
        $this->addPortNode($builder);
        $this->addDatabaseNode($builder);
        $this->addUserNode($builder);
        $this->addPasswordNode($builder);
        $this->addSshNode($builder);
        $this->addSslNode($builder);
        $this->addInitQueriesNode($builder);
    }

    protected function addDriverNode(NodeBuilder $builder): void
    {
        // For backward compatibility only
        $builder->scalarNode('driver');
    }

    protected function addHostNode(NodeBuilder $builder): void
    {
        $builder->scalarNode('host');
    }

    protected function addPortNode(NodeBuilder $builder): void
    {
        $builder->scalarNode('port');
    }

    protected function addDatabaseNode(NodeBuilder $builder): void
    {
        $builder->scalarNode('database')->cannotBeEmpty();
    }

    protected function addUserNode(NodeBuilder $builder): void
    {
        $builder->scalarNode('user')->isRequired();
    }

    protected function addPasswordNode(NodeBuilder $builder): void
    {
        $builder->scalarNode('#password')->isRequired();
    }

    protected function addSshNode(NodeBuilder $builder): void
    {
        $builder->append($this->sshNode);
    }

    protected function addSslNode(NodeBuilder $builder): void
    {
        $builder->append($this->sslNode);
    }

    protected function addInitQueriesNode(NodeBuilder $builder): void
    {
        $builder->arrayNode('initQueries')->prototype('scalar');
    }
}
