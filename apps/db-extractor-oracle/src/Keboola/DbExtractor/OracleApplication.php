<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Configuration\ConfigDefinition;

class OracleApplication extends Application
{
    public function __construct(array $config, $dataDir)
    {
        $config['parameters']['data_dir'] = $dataDir;
        $config['parameters']['extractor_class'] = 'Oracle';

        parent::__construct($config);

        $this->setConfigDefinition(new ConfigDefinition());
    }
}
