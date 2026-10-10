<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

/** What {@see TestResourceScope::close()} throws: the cleanup steps that failed, and what is still outstanding. */
final class TestResourceCleanupFailure extends \RuntimeException {}
