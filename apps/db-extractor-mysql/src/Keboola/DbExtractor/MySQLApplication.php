<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Configuration\NodeDefinition\MysqlDbNode;
use Keboola\DbExtractor\Configuration\NodeDefinition\MysqlTableNodesDecorator;
use Keboola\DbExtractorConfig\Config;
use Keboola\DbExtractorConfig\Configuration\ActionConfigRowDefinition;
use Keboola\DbExtractorConfig\Configuration\ConfigDefinition;
use Keboola\DbExtractorConfig\Configuration\ConfigRowDefinition;

class MySQLApplication extends Application
{
    protected function loadConfig(): void
    {
        $config = $this->getRawConfig();
        $action = $config['action'] ?? 'run';

        $config['parameters']['extractor_class'] = 'MySQL';
        $config['parameters']['data_dir'] = $this->getDataDir();
        $dbNode = new MysqlDbNode();

        if ($this->isRowConfiguration($config)) {
            if ($action === 'run') {
                $this->config = new Config(
                    $config,
                    new ConfigRowDefinition($dbNode, null, null, new MysqlTableNodesDecorator())
                );
            } else {
                $this->config = new Config($config, new ActionConfigRowDefinition($dbNode));
            }
        } else {
            $this->config = new Config(
                $config,
                new ConfigDefinition($dbNode, null, null, new MysqlTableNodesDecorator())
            );
        }
    }
}
