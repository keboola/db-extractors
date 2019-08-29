<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig;

use Keboola\Component\Config\BaseConfig;
use Keboola\Component\Config\BaseConfigDefinition;
use Keboola\DbExtractorConfig\Exception\UserException;
use Symfony\Component\Config\Definition\ConfigurationInterface;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Config\Definition\Processor;

class Config extends BaseConfig
{
    /** @var ConfigurationInterface */
    private $configDefinition;

    public function __construct(
        array $config,
        ?ConfigurationInterface $configDefinition = null
    ) {
        $this->setConfigDefinition($configDefinition);
        $this->setConfig($config);
    }

    /**
     * @param mixed[] $config
     */
    private function setConfig(array $config): void
    {
        try {
            $processor = new Processor();
            $processedConfig = $processor->processConfiguration($this->configDefinition, [$config]);
            $this->config = $processedConfig;
        } catch (InvalidConfigurationException $e) {
            throw new UserException($e->getMessage(), 0, $e);
        }
    }

    private function setConfigDefinition(?ConfigurationInterface $configDefinition): void
    {
        if ($configDefinition === null) {
            $configDefinition = new BaseConfigDefinition();
        }
        $this->configDefinition = $configDefinition;
    }
}
