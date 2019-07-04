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
            case self::CONFIG_ROW_DEFINITION:
            case self::CONFIG_DEFINITION:
                // @TODO
            case self::CONFIG_ROW_ACTION_DEFINITION:
                // @TODO
                break;
        }
        $this->configuration = $configuration;
    }

    private function isTableValid(array $table): bool
    {
        if (!array_key_exists('schema', $table)) {
            return false;
        }
        if (!array_key_exists('tableName', $table)) {
            return false;
        }
        if ($table['tableName'] === '') {
            return false;
        }
        return true;
    }

    private function validateTableParameters(array $table): void
    {
        if (isset($table['query']) && $table['query'] !== '') {
            if (isset($table['table'])) {
                throw new ConfigException(
                    sprintf(
                        'Invalid Configuration for "%s". Both table and query cannot be set together.',
                        $table['outputTable']
                    )
                );
            }
            if (isset($table['incrementalFetchingColumn'])) {
                throw new ConfigException(
                    sprintf(
                        'Invalid Configuration for "%s". Incremental fetching is not supported for advanced queries.',
                        $table['outputTable']
                    )
                );
            }
        } else if (!isset($table['table'])) {
            throw new ConfigException(
                sprintf(
                    'Invalid Configuration for "%s". One of table or query is required.',
                    $table['outputTable']
                )
            );
        } else if (!$this->isTableValid($table['table'])) {
            throw new ConfigException(
                sprintf(
                    'Invalid Configuration for "%s". The table property requires "tableName" and "schema"',
                    $table['outputTable']
                )
            );
        } else if (isset($table['incrementalFetching']['autoIncrementColumn']) && empty($table['primaryKey'])) {
            $this['logger']->warn("An import autoIncrement column is being used but output primary key is not set.");
        }
    }

    public function validateParameters(array $parameters, string $action = 'run'): array
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

            if ($action === 'run') {
                if (isset($processedParameters['tables'])) {
                    foreach ($processedParameters['tables'] as $table) {
                        $this->validateTableParameters($table);
                    }
                } else {
                    $this->validateTableParameters($processedParameters);
                }
            }

            return $processedParameters;
        } catch (ConfigException $e) {
            throw new \Keboola\DbExtractor\Exception\UserException($e->getMessage(), 0, $e);
        }
    }
}
