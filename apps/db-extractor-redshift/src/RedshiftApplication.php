<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Configuration\RedshiftTableNodeDecorator;
use Keboola\DbExtractorConfig\Config;
use Keboola\DbExtractorConfig\Configuration\ActionConfigRowDefinition;
use Keboola\DbExtractorConfig\Configuration\ConfigDefinition;
use Keboola\DbExtractorConfig\Configuration\ConfigRowDefinition;
use Psr\Log\LoggerInterface;

class RedshiftApplication extends Application
{
    public function __construct(array $config, LoggerInterface $logger, array $state, string $dataDir)
    {
        $config['parameters']['data_dir'] = $dataDir;
        $config['parameters']['extractor_class'] = 'Redshift';

        parent::__construct($config, $logger, $state);
    }

    protected function buildConfig(array $config): void
    {
        if ($this->isRowConfiguration($config)) {
            if ($this['action'] === 'run') {
                $configDefinition = new ConfigRowDefinition(
                    null,
                    null,
                    null,
                    new RedshiftTableNodeDecorator()
                );
            } else {
                $configDefinition = new ActionConfigRowDefinition();
            }
        } else {
            $configDefinition = new ConfigDefinition(
                null,
                null,
                null,
                new RedshiftTableNodeDecorator()
            );
        }
        $this->config = new Config($config, $configDefinition);
    }
}
