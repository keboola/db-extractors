<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Adapter;

use ArrayIterator;
use Keboola\DbExtractor\Adapter\Exception\AdapterSkippedException;
use Keboola\DbExtractor\Adapter\Exception\InvalidArgumentException;
use Keboola\DbExtractor\Adapter\Exception\InvalidStateException;
use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Psr\Log\LoggerInterface;
use Throwable;

/**
 * This class allows you to use multiple adapters (eg. BCP and PDO as a fallback) if one fails, next is tried.
 */
class FallbackExportAdapter implements ExportAdapter
{
    protected LoggerInterface $logger;

    /** @var ExportAdapter[] */
    protected array $adapters;

    /**
     * @param ExportAdapter[] $adapters
     */
    public function __construct(LoggerInterface $logger, array $adapters)
    {
        if (count($adapters) === 0) {
            throw new InvalidArgumentException('At least one adapter must be specified.');
        }

        $this->logger = $logger;
        $this->adapters = $adapters;
    }

    public function getName(): string
    {
        return 'fallback';
    }

    public function export(ExportConfig $exportConfig, string $csvFilePath): ExportResult
    {
        $iterator = new ArrayIterator($this->adapters);
        while ($iterator->valid()) {
            /** @var ExportAdapter $adapter */
            $adapter = $iterator->current();

            try {
                $this->logger->info(sprintf('Exporting by "%s" adapter.', $adapter->getName()));
                return $adapter->export($exportConfig, $csvFilePath);
            } catch (Throwable $e) {
                if ($e instanceof AdapterSkippedException) {
                    $this->logger->info(sprintf(
                        'Adapter "%s" skipped: %s',
                        $adapter->getName(),
                        $e->getMessage(),
                    ));
                } else {
                    $this->logger->warning(sprintf(
                        'Export by "%s" adapter failed: %s',
                        $adapter->getName(),
                        $e->getMessage(),
                    ));
                }

                // If is fallback adapter present -> log msg and continue
                $iterator->next();
                if ($iterator->valid()) {
                    continue;
                }

                throw $e;
            }
        }

        throw new InvalidStateException('At least one adapter must be specified.');
    }
}
