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

    public const CONFIG_DEFINITION = 'configDefinition';

    public const CONFIG_ROW_DEFINITION = 'configRowDefinition';

    public const CONFIG_ROW_ACTION_DEFINITION = 'configRowActionDefinition';

    public function __construct(ConfigurationInterface $configuration, string $type = self::CONFIG_DEFINITION)
    {
        switch ($type) {
            case self::CONFIG_ROW_DEFINITION:
            case self::CONFIG_DEFINITION:
                // @TODO
                break;
            case self::CONFIG_ROW_ACTION_DEFINITION:
                // @TODO
                break;
        }
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
