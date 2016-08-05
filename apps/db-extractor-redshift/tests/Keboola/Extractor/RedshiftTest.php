<?php

/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 10/12/15
 * Time: 14:25
 */

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Test\ExtractorTest;

class RedshiftTest extends ExtractorTest
{
    /** @var Application */
    protected $app;

    public function setUp()
    {
        if (!defined('APP_NAME')) {
            define('APP_NAME', 'ex-db-redshift');
        }
        $config = $this->getConfig();

        $pdo = new \PDO(
            "pgsql:dbname={$config['parameters']['db']['database']};port=5439;host=" . $config['parameters']['db']['host'],
            $config['parameters']['db']['user'],
            $config['parameters']['db']['password']
        );
        $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        $pdo->query("CREATE SCHEMA IF NOT EXISTS \"{$config['test']['schema']}\"");
        $pdo->query("DROP TABLE IF EXISTS \"{$config['test']['schema']}\".escaping;");
        $pdo->query("CREATE TABLE IF NOT EXISTS \"{$config['test']['schema']}\".escaping (col1 VARCHAR NOT NULL, col2 VARCHAR NOT NULL, col3 VARCHAR NOT NULL);");

        $credStr = "aws_access_key_id={$config['aws']['s3key']};aws_secret_access_key={$config['aws']['s3secret']}";

        $qry = "COPY \"{$config['test']['schema']}\".escaping ";
        $qry .= "FROM 's3://{$config["aws"]["bucket"]}/escaping.csv' CREDENTIALS '$credStr' DELIMITER ',' QUOTE '\"' CSV IGNOREHEADER 1";
        $pdo->query($qry);

        $this->app = new Application($this->getConfig());
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
        if (getenv('REDSHIFT_DB_SCHEMA')) {
            $config['test']['schema'] = getenv('REDSHIFT_DB_SCHEMA');
        }
        
        $config['parameters']['extractor_class'] = 'Redshift';
        return $config;
    }

    public function testRun()
    {
        $result = $this->app->run();
        $expectedCsvFile = $this->dataDir .  "/in/tables/escaping.csv";
        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv';
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest';
        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($outputManifestFile);;
        $this->assertEquals(file_get_contents($expectedCsvFile), file_get_contents($outputCsvFile));

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
