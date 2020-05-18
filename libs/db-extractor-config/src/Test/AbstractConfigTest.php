<?php

declare(strict_types=1);

namespace Keboola\DbExtractorConfig\Test;

use Keboola\DbExtractorConfig\Exception\UserException;
use PHPUnit\Framework\TestCase;

abstract class AbstractConfigTest extends TestCase
{
    public const CONFIG_FORMAT_JSON = 'json';

    protected string $dataDir = __DIR__ . '/../../tests/data';

    protected function getConfigDbNode(string $driver): array
    {
        return [
            'user' => $this->getEnv($driver, 'DB_USER', true),
            '#password' => $this->getEnv($driver, 'DB_PASSWORD', true),
            'host' => $this->getEnv($driver, 'DB_HOST'),
            'port' => $this->getEnv($driver, 'DB_PORT'),
            'database' => $this->getEnv($driver, 'DB_DATABASE'),
        ];
    }

    protected function getConfig(string $driver): array
    {
        $config = json_decode((string) file_get_contents($this->dataDir . '/' .$driver . '/config.json'), true);
        $config['parameters']['data_dir'] = $this->dataDir;
        $config['parameters']['db'] = $this->getConfigDbNode($driver);
        $config['parameters']['extractor_class'] = ucfirst($driver);

        return $config;
    }

    protected function getConfigRow(string $driver): array
    {
        $config = json_decode((string) file_get_contents($this->dataDir . '/' .$driver . '/configRow.json'), true);

        $config['parameters']['data_dir'] = $this->dataDir;
        $config['parameters']['db'] = $this->getConfigDbNode($driver);
        $config['parameters']['extractor_class'] = ucfirst($driver);

        return $config;
    }

    protected function getConfigRowForCsvErr(string $driver): array
    {
        $filename = $this->dataDir . '/' .$driver . '/configRowCsvErr.json';
        $config = json_decode((string) file_get_contents($filename), true);

        $config['parameters']['data_dir'] = $this->dataDir;
        $config['parameters']['db'] = $this->getConfigDbNode($driver);
        $config['parameters']['extractor_class'] = ucfirst($driver);

        return $config;
    }

    protected function getEnv(string $driver, string $suffix, bool $required = false): string
    {
        $env = strtoupper($driver) . '_' . $suffix;
        if ($required) {
            if (getenv($env) === false) {
                throw new \Exception($env . ' environment variable must be set.');
            }
        }
        return (string) getenv($env);
    }
}
