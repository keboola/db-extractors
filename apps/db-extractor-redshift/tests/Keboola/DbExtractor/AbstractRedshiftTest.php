<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Application;
use Keboola\DbExtractorLogger\Logger;
use Symfony\Component\Filesystem\Filesystem;
use Keboola\DbExtractor\Test\ExtractorTest;

abstract class AbstractRedshiftTest extends ExtractorTest
{
    protected const TESTING_SCHEMA_NAME = 'testing';

    protected const DRIVER = 'redshift';

    /** @var string  */
    protected $dataDir = __DIR__ . '/../../data';

    public function setUp(): void
    {
        $fs = new Filesystem();
        $fs->remove($this->dataDir . '/out/tables');
        $fs->mkdir($this->dataDir . '/out/tables');

        $this->initRedshiftData($this->getConfig(self::DRIVER));
    }

    private function initRedshiftData(array $config): void
    {
        $dsn = sprintf(
            'pgsql:dbname=%s;port=5439;host=%s',
            $config['parameters']['db']['database'],
            $config['parameters']['db']['host']
        );

        $pdo = new \PDO(
            $dsn,
            $config['parameters']['db']['user'],
            $config['parameters']['db']['#password']
        );
        $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        $pdo->query(sprintf('DROP SCHEMA IF EXISTS "%s" CASCADE', self::TESTING_SCHEMA_NAME));
        $pdo->query('CREATE SCHEMA "' . self::TESTING_SCHEMA_NAME. '"');
        $pdo->query('CREATE TABLE IF NOT EXISTS "' . self::TESTING_SCHEMA_NAME . "\".escaping 
                      (col1 VARCHAR NOT NULL DEFAULT 'a', 
                      col2 VARCHAR NOT NULL DEFAULT 'b', 
                      col3 VARCHAR NULL,
                      PRIMARY KEY (col1, col2));");

        $credStr = "aws_access_key_id={$config['aws']['s3key']};aws_secret_access_key={$config['aws']['s3secret']}";

        $qry = sprintf('COPY "%s".escaping ', self::TESTING_SCHEMA_NAME);
        $qry .= sprintf(
            "FROM 's3://%s/escaping.csv' CREDENTIALS '%s' DELIMITER ',' QUOTE '\"' CSV IGNOREHEADER 1",
            $config['aws']['bucket'],
            $credStr
        );
        $pdo->query($qry);
    }

    public function getConfig(string $driver = self::DRIVER): array
    {
        $config = parent::getConfig($driver);
        if (getenv('AWS_ACCESS_KEY')) {
            $config['aws']['s3key'] = getenv('AWS_ACCESS_KEY');
        }
        if (getenv('AWS_SECRET_KEY')) {
            $config['aws']['s3secret'] = getenv('AWS_SECRET_KEY');
        }
        if (getenv('AWS_REGION')) {
            $config['aws']['region'] = getenv('AWS_REGION');
        }
        if (getenv('AWS_S3_BUCKET')) {
            $config['aws']['bucket'] = getenv('AWS_S3_BUCKET');
        }

        $config['parameters']['extractor_class'] = 'Redshift';
        return $config;
    }

    public function getConfigRow(string $driver): array
    {
        $config = parent::getConfigRow($driver);
        if (getenv('AWS_ACCESS_KEY')) {
            $config['aws']['s3key'] = getenv('AWS_ACCESS_KEY');
        }
        if (getenv('AWS_SECRET_KEY')) {
            $config['aws']['s3secret'] = getenv('AWS_SECRET_KEY');
        }
        if (getenv('AWS_REGION')) {
            $config['aws']['region'] = getenv('AWS_REGION');
        }
        if (getenv('AWS_S3_BUCKET')) {
            $config['aws']['bucket'] = getenv('AWS_S3_BUCKET');
        }

        $config['parameters']['extractor_class'] = 'Redshift';
        return $config;
    }

    public function createApplication(array $config): Application
    {
        return new Application($config, new Logger('ex-db-redshift-tests'));
    }

    public function configProvider(): array
    {
        $this->dataDir = __DIR__ . '/../../data';
        return [
            [
                $this->getConfig(self::DRIVER),
            ],
            [
                $this->getConfigRow(self::DRIVER),
            ],
        ];
    }

    public function getPrivateKey(): string
    {
        return (string) file_get_contents('/root/.ssh/id_rsa');
    }

    public function getPublicKey(): string
    {
        return (string) file_get_contents('/root/.ssh/id_rsa.pub');
    }
}
