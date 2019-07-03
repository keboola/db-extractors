<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig;

use Keboola\DbExtractorConfig\Exception\UserException;
use Symfony\Component\Config\Definition\ConfigurationInterface;
use Symfony\Component\Config\Definition\Exception\Exception as ConfigException;
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
            case self::CONFIG_DEFINITION:
                $configuration
                    ->getConfigTreeBuilder()
                    ->root('parameters')
                    ->validate()
                        ->ifTrue(function ($v) {
                            if (isset($v['query']) && $v['query'] !== '' && isset($v['table'])) {
                                return true;
                            }
                            return false;
                        })->thenInvalid('Both table and query cannot be set together.')
                    ->end()
                    ->validate()
                        ->ifTrue(function ($v) {
                            if (isset($v['query']) && $v['query'] !== '' && isset($v['incrementalFetchingColumn'])) {
                                return true;
                            }
                            return false;
                        })->thenInvalid('Incremental fetching is not supported for advanced queries.')
                    ->end()
                    ->validate()
                        ->ifTrue(function ($v) {
                            if (!isset($v['table']) && !isset($v['query'])) {
                                return true;
                            }
                            return false;
                        })->thenInvalid('One of table or query is required')
                    ->end();
                break;
            case self::CONFIG_ROW_DEFINITION:
                // @TODO
                break;
            case self::CONFIG_ROW_ACTION_DEFINITION:
                // @TODO
                break;
        }
        $this->configuration = $configuration;
    }


    public function validateParameters(array $parameters): array
    {
        try {
            $processor = new Processor();
            $processedParameters = $processor->processConfiguration(
                $this->configuration,
                [$parameters]
            );

            if (!empty($processedParameters['db']['#password'])) {
                $processedParameters['db']['password'] = $processedParameters['db']['#password'];
            }

            return $processedParameters;
        } catch (ConfigException $e) {
            throw new UserException($e->getMessage(), 0, $e);
        }
    }
}
