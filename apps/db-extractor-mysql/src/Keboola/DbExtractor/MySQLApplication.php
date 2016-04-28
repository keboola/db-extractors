<?php
/**
 * @package ex-db-mysql
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Configuration\MySQLConfigDefinition;

class MySQLApplication extends Application
{
	public function __construct(array $config, $dataDir)
	{
		$config['parameters']['data_dir'] = $dataDir;
		$config['parameters']['extractor_class'] = 'MySQL';

		parent::__construct($config);

		$this->setConfigDefinition(new MySQLConfigDefinition());
	}
}