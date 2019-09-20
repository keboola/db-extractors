<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Configuration\OracleGetTablesDefinition;
use Keboola\DbExtractorConfig\Config;
use Keboola\DbExtractorConfig\Configuration\ConfigDefinition;
use Keboola\DbExtractorLogger\Logger;

class OracleApplication extends Application
{
    public function __construct(array $config, Logger $logger, array $state, string $dataDir)
    {
        $config['parameters']['data_dir'] = $dataDir;
        $config['parameters']['extractor_class'] = 'Oracle';

        parent::__construct($config, $logger, $state);
    }

    public function buildConfig(array $config): void
    {
        if ($this['action'] === 'getTables') {
            $this->config = new Config($config, new OracleGetTablesDefinition());
        } else {
            $this->config = new Config($config, new ConfigDefinition());
        }
    }
}
