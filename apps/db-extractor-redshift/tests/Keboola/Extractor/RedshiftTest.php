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
        $this->app = new Application($this->getConfig());
    }

    public function getConfig($driver = 'redshift')
    {
        $config = parent::getConfig($driver);
        $config['parameters']['extractor_class'] = 'Redshift';
        return $config;
    }

    public function testRun()
    {
        $result = $this->app->run();

        // TODO: test this thing
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
