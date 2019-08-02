<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig;

use Keboola\DbExtractorConfig\Exception\UserException;
use Symfony\Component\Config\Definition\ConfigurationInterface;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Config\Definition\Processor;

class Config
{
    /** @var ConfigurationInterface $configuration */
    private $configuration;

    public function __construct(ConfigurationInterface $configuration)
    {
        $this->configuration = $configuration;
    }

    public function validateParameters(array $parameters, string $action = 'run'): array
    {
        try {
            $processor = new Processor();
            $processedParameters = $processor->processConfiguration($this->configuration, [$parameters]);

            if (!empty($processedParameters['db']['#password'])) {
                $processedParameters['db']['password'] = $processedParameters['db']['#password'];
            }
            return $processedParameters;
        } catch (InvalidConfigurationException $e) {
            throw new UserException($e->getMessage(), 0, $e);
        }
    }
}
