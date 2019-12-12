<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Configuration\NodeDefinition\MysqlDbNode;
use Keboola\DbExtractor\Configuration\NodeDefinition\MysqlTablesNode;
use Keboola\DbExtractorConfig\Configuration\ActionConfigRowDefinition;
use Keboola\DbExtractorConfig\Configuration\ConfigDefinition;
use Keboola\DbExtractorConfig\Configuration\ConfigRowDefinition;
use Keboola\DbExtractorLogger\Logger;
use Keboola\DbExtractorConfig\Config;

class MySQLApplication extends Application
{
    public function __construct(array $config, ?Logger $logger = null, array $state = [], string $dataDir = '/data/')
    {
        $config['parameters']['data_dir'] = $dataDir;
        $config['parameters']['extractor_class'] = 'MySQL';

        parent::__construct($config, ($logger) ? $logger : new Logger('ex-db-mysql'), $state);
    }

    protected function buildConfig(array $config): void
    {
        $dbNode = new MysqlDbNode();
        if ($this->isRowConfiguration($config)) {
            if ($this['action'] === 'run') {
                $this->config = new Config($config, new ConfigRowDefinition($dbNode));
            } else {
                $this->config = new Config($config, new ActionConfigRowDefinition($dbNode));
            }
        } else {
            $this->config = new Config($config, new ConfigDefinition($dbNode, null, new MysqlTablesNode()));
        }
    }
}
