<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Component\JsonHelper;
use Keboola\DbExtractor\Exception\BadUsernameException;
use Keboola\DbExtractor\OracleApplication;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

class CheckUsernameTest extends TestCase
{
    protected string $dataDir = __DIR__ . '/data';

    protected function setUp(): void
    {
        putenv('KBC_DATADIR=' . $this->dataDir);

        parent::setUp();
    }

    public function testValidCheckUsername(): void
    {
        $logger = new TestLogger();
        $config = $this->createConfig();
        $config['image_parameters']['checkUsername'] = [
            'enabled' => true,
        ];
        putenv('KBC_REALUSER=' . (string) getEnv('ORACLE_DB_USER'));

        JsonHelper::writeFile($this->dataDir . '/config.json', $config);

        new OracleApplication($logger);

        Assert::assertTrue(
            $logger->hasInfoThatContains(sprintf(
                'Your username "%s" and database username are same. Running allowed.',
                (string) getEnv('ORACLE_DB_USER')
            ))
        );
    }

    public function testInvalidCheckUsername(): void
    {
        $logger = new TestLogger();
        $config = $this->createConfig();
        $config['image_parameters']['checkUsername'] = [
            'enabled' => true,
        ];
        putenv('KBC_REALUSER=dbUsername');

        JsonHelper::writeFile($this->dataDir . '/config.json', $config);

        $this->expectException(BadUsernameException::class);
        $this->expectExceptionMessage(sprintf(
            'Your username "dbUsername" does not have permission ' .
            'to run configuration with the database username "%s"',
            (string) getEnv('ORACLE_DB_USER')
        ));
        new OracleApplication($logger);
    }

    public function testServiceAccountRegexpMatch(): void
    {
        $logger = new TestLogger();
        $config = $this->createConfig();
        $config['parameters']['db']['user'] = 'service__abc';
        $config['image_parameters']['checkUsername'] = [
            'enabled' => true,
            'serviceAccountRegexp' => '~^service__~i',
        ];
        putenv('KBC_REALUSER=dbUsername');

        JsonHelper::writeFile($this->dataDir . '/config.json', $config);

        new OracleApplication($logger);

        Assert::assertTrue(
            $logger->hasInfoThatContains('Database username "service__abc" is service account, username check skipped.')
        );
    }

    public function testServiceAccountRegexpDontMatch(): void
    {
        $logger = new TestLogger();
        $config = $this->createConfig();
        $config['parameters']['db']['user'] = 'user123';
        $config['image_parameters']['checkUsername'] = [
            'enabled' => true,
            'serviceAccountRegexp' => '~^service__~i',
        ];
        putenv('KBC_REALUSER=dbUsername');

        JsonHelper::writeFile($this->dataDir . '/config.json', $config);

        $this->expectException(BadUsernameException::class);
        $this->expectExceptionMessage(
            'Your username "dbUsername" does not have permission ' .
            'to run configuration with the database username "user123"'
        );
        new OracleApplication($logger);
    }

    public function testUserAccountRegexpMatch(): void
    {
        $logger = new TestLogger();
        $config = $this->createConfig();
        $config['parameters']['db']['user'] = 'user_abc';
        $config['image_parameters']['checkUsername'] = [
            'enabled' => true,
            'userAccountRegexp' => '~^user_~i',
        ];
        putenv('KBC_REALUSER=dbUsername');

        JsonHelper::writeFile($this->dataDir . '/config.json', $config);

        $this->expectException(BadUsernameException::class);
        $this->expectExceptionMessage(
            'Your username "dbUsername" does not have permission ' .
            'to run configuration with the database username "user_abc"'
        );
        new OracleApplication($logger);
    }

    public function testUserAccountRegexpDontMatch(): void
    {
        $logger = new TestLogger();
        $config = $this->createConfig();
        $config['parameters']['db']['user'] = 'service__abc';
        $config['image_parameters']['checkUsername'] = [
            'enabled' => true,
            'userAccountRegexp' => '~^user_~i',
        ];
        putenv('KBC_REALUSER=dbUsername');

        JsonHelper::writeFile($this->dataDir . '/config.json', $config);

        new OracleApplication($logger);

        Assert::assertTrue(
            $logger->hasInfoThatContains('Database username "service__abc" is service account, username check skipped.')
        );
    }

    public function testDisabledChecking(): void
    {
        $logger = new TestLogger();
        $config = $this->createConfig();
        putenv('KBC_REALUSER=' . (string) getEnv('ORACLE_DB_USER'));

        JsonHelper::writeFile($this->dataDir . '/config.json', $config);

        new OracleApplication($logger);

        Assert::assertEquals([
            [
                'level' => 'debug',
                'message' => 'Component initialization completed',
                'context' => [],
            ],
        ], $logger->records);
    }

    private function createConfig(string $action = 'run'): array
    {
        $config = [
            'parameters' => [
                'data_dir' => 'testDatadir',
                'extractor_class' => 'test',
                'db' => [
                    'host' => (string) getEnv('ORACLE_DB_HOST'),
                    'port' => (string) getEnv('ORACLE_DB_PORT'),
                    'database' => (string) getEnv('ORACLE_DB_DATABASE'),
                    'user' => (string) getEnv('ORACLE_DB_USER'),
                    '#password' => (string) getEnv('ORACLE_DB_PASSWORD'),
                ],
                'id' => 123,
                'name' => 'row_name',
                'table' => [
                    'tableName' => 'simple',
                    'schema' => (string) getEnv('ORACLE_DB_DATABASE'),
                ],
                'outputTable' => 'output',
            ],
            'image_parameters' => [],
        ];

        if ($action !== 'run') {
            $config['action'] = $action;
        }

        return $config;
    }
}
