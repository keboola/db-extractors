<?php

declare(strict_types=1);

date_default_timezone_set('Europe/Prague');
ini_set('display_errors', '1');
error_reporting(E_ALL);

$basedir = dirname(__DIR__);

require_once $basedir . '/vendor/autoload.php';

$client =  new \Aws\S3\S3Client([
    'region' => getenv('AWS_REGION'),
    'version' => '2006-03-01',
    'credentials' => [
        'key' => getenv('AWS_ACCESS_KEY'),
        'secret' => getenv('AWS_SECRET_KEY'),
    ],
]);

// Where the files will be source from
$source = $basedir . '/tests/phpunit/data/in/tables';

// Where the files will be transferred to
$bucket = getenv('AWS_S3_BUCKET');
$dest = 's3://' . $bucket;

// clear bucket
$result = $client->listObjects([
    'Bucket' => $bucket,
    'Delimiter' => '/',
]);

$objects = $result->get('Contents');
if ($objects) {
    $client->deleteObjects([
        'Bucket' => $bucket,
        'Delete' => [
            'Objects' => array_map(function ($object) {
                return [
                    'Key' => $object['Key'],
                ];
            }, $objects),
        ],
    ]);
}

// Create a transfer object.
$manager = new \Aws\S3\Transfer($client, $source, $dest, [
    'debug' => true,
]);


// Perform the transfer synchronously.
$manager->transfer();

echo "Data loaded OK\n";
