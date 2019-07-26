<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Configuration\ConfigDefinition;
use Keboola\DbExtractor\Configuration\OracleGetTablesDefinition;

class OracleApplication extends Application
{
    public function __construct(array $config, Logger $logger, array $state, string $dataDir)
    {
        $config['parameters']['data_dir'] = $dataDir;
        $config['parameters']['extractor_class'] = 'Oracle';

        parent::__construct($config, $logger, $state);

        $this->setConfigDefinition(new ConfigDefinition());

        if ($this['action'] === 'getTables') {
            $this->setConfigDefinition(new OracleGetTablesDefinition());
        }
    }
}
