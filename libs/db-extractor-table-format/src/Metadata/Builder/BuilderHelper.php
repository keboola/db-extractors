<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TableResultFormat\Metadata\Builder;

use Keboola\Utils\Sanitizer\ColumnNameSanitizer;

class BuilderHelper
{
    public static function sanitizeName(string $name): string
    {
        $sanitized = ColumnNameSanitizer::sanitize($name);

        // In some databases, eg. MsSQL is one space valid column name, so we must it sanitize to non-empty string
        if ($sanitized === '') {
            // Name cannot start/end with special char, it is trim in ColumnNameSanitizer
            $sanitized = ColumnNameSanitizer::sanitize(
                'empty' . preg_replace('~\s~', '_', $name) . 'name'
            );
        }

        return $sanitized;
    }
}
