<?php

/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 10/12/15
 * Time: 14:25
 */

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Test\ExtractorTest;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Yaml\Yaml;

class RedshiftTest extends ExtractorTest
{
    const TESTING_SCHEMA_NAME = 'testing';

    public function setUp()
    {
        $fs = new Filesystem();
        $fs->remove($this->dataDir . '/out/tables');
        $fs->mkdir($this->dataDir . '/out/tables');

        if (!defined('APP_NAME')) {
            define('APP_NAME', 'ex-db-redshift');
        }
        $config = $this->getConfig();

        $pdo = new \PDO(
            "pgsql:dbname={$config['parameters']['db']['database']};port=5439;host=" . $config['parameters']['db']['host'],
            $config['parameters']['db']['user'],
            $config['parameters']['db']['#password']
        );
        $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        $pdo->query("CREATE SCHEMA IF NOT EXISTS \"" . self::TESTING_SCHEMA_NAME. "\"");
        $pdo->query("DROP TABLE IF EXISTS \"" . self::TESTING_SCHEMA_NAME. "\".escaping;");
        $pdo->query("CREATE TABLE IF NOT EXISTS \"" . self::TESTING_SCHEMA_NAME. "\".escaping (col1 VARCHAR NOT NULL, col2 VARCHAR NOT NULL, col3 VARCHAR NOT NULL);");

        $credStr = "aws_access_key_id={$config['aws']['s3key']};aws_secret_access_key={$config['aws']['s3secret']}";

        $qry = "COPY \"" . self::TESTING_SCHEMA_NAME. "\".escaping ";
        $qry .= "FROM 's3://{$config["aws"]["bucket"]}/escaping.csv' CREDENTIALS '$credStr' DELIMITER ',' QUOTE '\"' CSV IGNOREHEADER 1";
        $pdo->query($qry);
    }

    public function getConfig($driver = 'redshift')
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

    private function runApp(Application $app)
    {
        $result = $app->run();
        $expectedCsvFile = $this->dataDir .  "/in/tables/escaping.csv";
        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv';
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest';
        $manifest = Yaml::parse(file_get_contents($outputManifestFile));

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($outputManifestFile);;
        $this->assertEquals(file_get_contents($expectedCsvFile), file_get_contents($outputCsvFile));
        $this->assertEquals('in.c-main.escaping', $manifest['destination']);
        $this->assertEquals(true, $manifest['incremental']);
        $this->assertEquals('col3', $manifest['primary_key'][0]);
    }

    public function testRun()
    {
        $this->runApp(new Application($this->getConfig()));
    }

    public function testRunWithSSH()
    {
        $config = $this->getConfig();
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getEnv('redshift', 'DB_SSH_KEY_PRIVATE'),
                'public' => $this->getEnv('redshift', 'DB_SSH_KEY_PUBLIC')
            ],
            'user' => 'root',
            'sshHost' => 'sshproxy',
            'localPort' => '33306',
            'remoteHost' => $this->getEnv('redshift', 'DB_HOST'),
            'remotePort' => $this->getEnv('redshift', 'DB_PORT')
        ];
        $this->runApp(new Application($config));
    }

    public function testTestConnection()
    {
        $config = $this->getConfig();
        $config['action'] = 'testConnection';

        $app = new Application($config);
        $result = $app->run();
        $this->assertEquals('success', $result['status']);
    }
}
