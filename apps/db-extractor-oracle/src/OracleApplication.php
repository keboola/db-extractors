<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Configuration\NodeDefinition\OracleDbNode;
use Keboola\DbExtractorConfig\Configuration\GetTablesListFilterDefinition;
use Psr\Log\LoggerInterface;
use Keboola\DbExtractorConfig\Config;
use Keboola\DbExtractorConfig\Configuration\ActionConfigRowDefinition;
use Keboola\DbExtractorConfig\Configuration\ConfigDefinition;
use Keboola\DbExtractorConfig\Configuration\ConfigRowDefinition;

class OracleApplication extends Application
{
    public function __construct(array $config, LoggerInterface $logger, array $state, string $dataDir)
    {
        $config['parameters']['data_dir'] = $dataDir;
        $config['parameters']['extractor_class'] = 'Oracle';
        if (isset($config['image_parameters']['db']['defaultRowPrefetch'])) {
            $config['parameters']['db']['defaultRowPrefetch'] = $config['image_parameters']['db']['defaultRowPrefetch'];
        }

        parent::__construct($config, $logger, $state);
    }

    public function buildConfig(array $config): void
    {
        if ($this['action'] === 'getTables') {
            $this->config = new Config($config, new GetTablesListFilterDefinition(new OracleDbNode()));
        } elseif ($this->isRowConfiguration($config)) {
            if ($this['action'] === 'run') {
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
