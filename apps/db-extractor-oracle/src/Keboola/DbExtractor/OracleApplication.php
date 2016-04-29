<?php
/**
 * @package ex-db-oracle
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Configuration\OracleConfigDefinition;

class OracleApplication extends Application
{
	public function __construct(array $config, $dataDir)
	{
		$config['parameters']['data_dir'] = $dataDir;
		$config['parameters']['extractor_class'] = 'Oracle';

		parent::__construct($config);

		$this->setConfigDefinition(new OracleConfigDefinition());
	}
}
