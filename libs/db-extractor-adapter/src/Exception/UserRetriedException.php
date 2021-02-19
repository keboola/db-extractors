<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter\Exception;

use Throwable;

class UserRetriedException extends UserException
{
    private int $tryCount;

    public function __construct(int $tryCount, string $message = '', int $code = 0, ?Throwable $previous = null)
    {
        parent::__construct($message, $code, $previous);
        $this->tryCount = $tryCount;
    }

    public function getTryCount(): int
    {
        return $this->tryCount;
    }
}
