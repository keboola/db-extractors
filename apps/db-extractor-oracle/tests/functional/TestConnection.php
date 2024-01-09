<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\FunctionalTests;

use Exception;
use Keboola\DbExtractor\Configuration\OracleDatabaseConfig;
use Throwable;

class TestConnection
{
    /** @var resource $connection */
    private $connection;

    public static function getDbConfigArray(): array
    {
        return [
            'host' => (string) getenv('ORACLE_DB_HOST'),
            'port' => (string) getenv('ORACLE_DB_PORT'),
            'user' => (string) getenv('ORACLE_DB_USER'),
            '#password' => (string) getenv('ORACLE_DB_PASSWORD'),
            'database' => (string) getenv('ORACLE_DB_DATABASE'),
        ];
    }

    public static function createDbConfig(): OracleDatabaseConfig
    {
        $dbConfig = self::getDbConfigArray();
        return OracleDatabaseConfig::fromArray($dbConfig);
    }

    public static function createConnection(): self
    {
        return new self();
    }

    public function __construct()
    {
        $databaseConfig = self::createDbConfig();

        $dbString = sprintf(
            '//%s:%s/%s',
            $databaseConfig->getHost(),
            $databaseConfig->getPort(),
            $databaseConfig->getDatabase(),
        );

        $adminConnection = @oci_connect(
            'system',
            'oracle',
            $dbString,
            'AL32UTF8',
        );

        if (!$adminConnection) {
            $error = (array) oci_error();
            throw new Exception('ADMIN CONNECTION ERROR: ' . $error['message']);
        }
        $this->connection = $adminConnection;

        $this->createTestUser($databaseConfig);

        oci_close($adminConnection);

        $connection = @oci_connect(
            $databaseConfig->getUsername(),
            $databaseConfig->getPassword(),
            $dbString,
            'AL32UTF8',
        );
        if (!$connection) {
            $error = (array) oci_error();
            throw new Exception('CONNECTION ERROR: ' . $error['message']);
        }
        $this->connection = $connection;
    }

    public function exec(string $sql): void
    {
        $stmt = oci_parse($this->connection, $sql);
        if (!$stmt) {
            throw new Exception(sprintf('Can\'t parse sql statement %s', $sql));
        }
        try {
            oci_execute($stmt);
        } catch (Throwable $e) {
            throw $e;
        } finally {
            oci_free_statement($stmt);
        }
    }

    private function createTestUser(OracleDatabaseConfig $dbConfig): void
    {
        try {
            // create test user
            $this->exec(
                sprintf(
                    'CREATE USER %s IDENTIFIED BY %s DEFAULT TABLESPACE users',
                    $dbConfig->getUsername(),
                    $dbConfig->getPassword(),
                ),
            );

            // provide roles
            $this->exec(
                sprintf(
                    'GRANT CONNECT,RESOURCE,DBA TO %s',
                    $dbConfig->getUsername(),
                ),
            );

            // grant privileges
            $this->exec(
                sprintf(
                    'GRANT CREATE SESSION GRANT ANY PRIVILEGE TO %s',
                    $dbConfig->getUsername(),
                ),
            );
        } catch (Throwable $e) {
            // make sure this is the case that TESTER already exists
            if (!strstr($e->getMessage(), 'ORA-01920')) {
                echo "\nCreate test user error: " . $e->getMessage() . "\n";
                echo "\nError code: " . $e->getCode() . "\n";
            }
        }
    }
}
