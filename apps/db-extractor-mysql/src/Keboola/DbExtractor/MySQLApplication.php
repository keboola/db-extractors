<?php
/**
 * @package ex-db-mysql
 * @author Erik Zigo <erik.zigo@keboola.com>
 */

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Configuration\MySQLConfigDefinition;
use Keboola\DbExtractor\Configuration\MySQLConfigRowDefinition;

class MySQLApplication extends Application
{
    public function __construct(array $config, $dataDir)
    {
        parent::__construct($config);

        $config['parameters']['data_dir'] = $dataDir;
        $config['parameters']['extractor_class'] = 'MySQL';

        if (isset($this['parameters']['tables'])) {
            $this->setConfigDefinition(new MySQLConfigDefinition());
        } else {
            $this->setConfigDefinition(new MySQLConfigRowDefinition());
        }
    }
}
