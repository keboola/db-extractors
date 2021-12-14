<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Adapter\Exception\OdbcException;
use Keboola\DbExtractor\Adapter\ODBC\OdbcConnection;
use Throwable;

class SnowflakeOdbcConnection extends OdbcConnection
{
    use QuoteTrait;

    public function testConnection(): void
    {
        $this->query('SELECT current_date;');
    }

    protected function handleConnectionError(string $error, int $code = 0, ?Throwable $previousException = null): void
    {
        if (strpos(odbc_errormsg(), 'msg=')) {
            preg_match('/msg=\'(.*)\'\./', odbc_errormsg(), $message);
            throw new OdbcException($message[1] . ' ' . odbc_error());
        }
    }
}
