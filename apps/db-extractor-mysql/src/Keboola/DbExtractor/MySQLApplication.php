<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Configuration\MySQLConfigDefinition;
use Keboola\DbExtractor\Configuration\MySQLConfigRowDefinition;

class MySQLApplication extends Application
{
    public function __construct(array $config, ?Logger $logger = null, array $state = [], string $dataDir = '/data/')
    {
        $config['parameters']['data_dir'] = $dataDir;
        $config['parameters']['extractor_class'] = 'MySQL';

        parent::__construct($config, ($logger) ? $logger : new Logger("ex-db-mysql"), $state);

        if (isset($this['parameters']['tables'])) {
            $this->setConfigDefinition(new MySQLConfigDefinition());
        } else {
            $this->setConfigDefinition(new MySQLConfigRowDefinition());
        }
    }
}
