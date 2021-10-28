<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration\NodeDefinition;

use Keboola\DbExtractorConfig\Configuration\NodeDefinition\DbNode;
use Symfony\Component\Config\Definition\Builder\NodeBuilder;

class MysqlDbNode extends DbNode
{

    public const TRANSACTION_LEVEL_REPEATABLE_READ = 'REPEATABLE READ';
    public const TRANSACTION_LEVEL_READ_COMMITTED = 'READ COMMITTED';
    public const TRANSACTION_LEVEL_READ_UNCOMMITTED = 'READ UNCOMMITTED';
    public const TRANSACTION_LEVEL_SERIALIZABLE = 'SERIALIZABLE';

    protected function init(NodeBuilder $builder): void
    {
        parent::init($builder);
        $this->addNetworkCompression($builder);
        $this->addTransactionIsolationLevel($builder);
    }

    protected function addInitQueriesNode(NodeBuilder $builder): void
    {
       // not implemented
    }

    protected function addDatabaseNode(NodeBuilder $builder): void
    {
        // Database can be empty
        $builder->scalarNode('database');
    }

    protected function addNetworkCompression(NodeBuilder $builder): void
    {
        $builder->booleanNode('networkCompression')->defaultValue(false)->end();
    }

    protected function addTransactionIsolationLevel(NodeBuilder $builder): void
    {
        $builder->enumNode('transactionIsolationLevel')->values([
            self::TRANSACTION_LEVEL_REPEATABLE_READ,
            self::TRANSACTION_LEVEL_READ_COMMITTED,
            self::TRANSACTION_LEVEL_READ_UNCOMMITTED,
            self::TRANSACTION_LEVEL_SERIALIZABLE,
        ])->end();
    }
}
