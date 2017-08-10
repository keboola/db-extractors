<?php
/**
 * Created by PhpStorm.
 * User: marc
 * Date: 13/06/2017
 * Time: 19:02
 */

namespace Keboola\DbExtractor;

use Symfony\Component\Filesystem\Filesystem;
use Keboola\DbExtractor\Test\ExtractorTest;

abstract class AbstractRedshiftTest extends ExtractorTest
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
        $config = $this->getConfig('redshift');

        $pdo = new \PDO(
            "pgsql:dbname={$config['parameters']['db']['database']};port=5439;host=" . $config['parameters']['db']['host'],
            $config['parameters']['db']['user'],
            $config['parameters']['db']['#password']
        );
        $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        $pdo->query("CREATE SCHEMA IF NOT EXISTS \"" . self::TESTING_SCHEMA_NAME. "\"");
        $pdo->query("DROP TABLE IF EXISTS \"" . self::TESTING_SCHEMA_NAME. "\".escaping;");
        $pdo->query("CREATE TABLE IF NOT EXISTS \"" . self::TESTING_SCHEMA_NAME . "\".escaping 
                      (col1 VARCHAR NOT NULL DEFAULT 'a', 
                      col2 VARCHAR NOT NULL DEFAULT 'b', 
                      col3 VARCHAR NULL,
                      PRIMARY KEY (col1, col2));");

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

}