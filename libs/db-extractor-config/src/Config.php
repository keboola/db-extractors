<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig;

use Keboola\Component\Config\BaseConfig;
use Keboola\DbExtractorConfig\Exception\UserException;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class Config extends BaseConfig
{

    /**
     * @param mixed[] $config
     */
    protected function setConfig(array $config): void
    {
        try {
            parent::setConfig($config);
        } catch (InvalidConfigurationException $e) {
            throw new UserException($e->getMessage(), 0, $e);
        }
    }
}
