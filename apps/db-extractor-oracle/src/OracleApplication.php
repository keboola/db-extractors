<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Configuration\NodeDefinition\OracleDbNode;
use Keboola\DbExtractorConfig\Config;
use Keboola\DbExtractorConfig\Configuration\ActionConfigRowDefinition;
use Keboola\DbExtractorConfig\Configuration\ConfigDefinition;
use Keboola\DbExtractorConfig\Configuration\ConfigRowDefinition;
use Keboola\DbExtractorConfig\Configuration\GetTablesListFilterDefinition;

class OracleApplication extends Application
{
    public function loadConfig(): void
    {
        $config = $this->getRawConfig();
        $action = $config['action'] ?? 'run';

        $config['parameters']['extractor_class'] = 'Oracle';
        $config['parameters']['data_dir'] = $this->getDataDir();

        if (isset($config['image_parameters']['db']['defaultRowPrefetch'])) {
            $config['parameters']['db']['defaultRowPrefetch'] = $config['image_parameters']['db']['defaultRowPrefetch'];
        }

        if ($action === 'getTables') {
            $this->config = new Config($config, new GetTablesListFilterDefinition(new OracleDbNode()));
        } elseif ($this->isRowConfiguration($config)) {
            if ($action === 'run') {
                $config['parameters']['id'] = 1;
                $config['parameters']['name'] = $config['parameters']['outputTable'];
                $this->config = new Config($config, new ConfigRowDefinition(new OracleDbNode()));
            } else {
                $this->config = new Config($config, new ActionConfigRowDefinition(new OracleDbNode()));
            }
        } else {
            $this->config = new Config($config, new ConfigDefinition(new OracleDbNode()));
        }
    }

    protected function isRowConfiguration(array $config): bool
    {
        if (isset($config['parameters']['table']) || isset($config['parameters']['query'])) {
            return true;
        }

        return false;
    }
}
