<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Configuration\RedshiftTableNodeDecorator;
use Keboola\DbExtractorConfig\Config;
use Keboola\DbExtractorConfig\Configuration\ActionConfigRowDefinition;
use Keboola\DbExtractorConfig\Configuration\ConfigDefinition;
use Keboola\DbExtractorConfig\Configuration\ConfigRowDefinition;

class RedshiftApplication extends Application
{
    protected function loadConfig(): void
    {
        $config = $this->getRawConfig();
        $action = $config['action'] ?? 'run';

        $config['parameters']['extractor_class'] = 'Redshift';
        $config['parameters']['data_dir'] = $this->getDataDir();

        if ($this->isRowConfiguration($config)) {
            if ($action === 'run') {
                $configDefinition = new ConfigRowDefinition(
                    null,
                    null,
                    null,
                    new RedshiftTableNodeDecorator(),
                );
            } else {
                $configDefinition = new ActionConfigRowDefinition();
            }
        } else {
            $configDefinition = new ConfigDefinition(
                null,
                null,
                null,
                new RedshiftTableNodeDecorator(),
            );
        }
        $this->config = new Config($config, $configDefinition);
    }
}
