<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Configuration\ConfigDefinition;

class OracleApplication extends Application
{
    public function __construct(array $config, ?Logger $logger = null, array $state = [], string $dataDir = '/data/')
    {
        $config['parameters']['data_dir'] = $dataDir;
        $config['parameters']['extractor_class'] = 'Oracle';
        $logger = $logger ?? new Logger("ex-db-oracle");
        parent::__construct($config, $logger, $state);

        $this->setConfigDefinition(new ConfigDefinition());
    }
}
