<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\CancelledException;
use IDCT\NATS\JetStream\Consumers\PullInboxRejection;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\delay;

/**
 * The rejection state of one pull consumer run's inbox (#206, PullInboxRejection): what the inbox's rejection handler
 * records, whichever fiber's read meets the -ERR, and the wake-up that ends the engine's waits.
 */
final class PullInboxRejectionTest extends TestCase
{
    /**
     * Nothing is recorded and the wake-up has not fired until the first rejection, which is then kept, with the
     * wake-up fired; a second rejection changes neither.
     */
    public function testItKeepsTheFirstRejectionWithItsWakeUpFired(): void
    {
        $rejection = new PullInboxRejection();

        self::assertNull($rejection->error());
        self::assertFalse($rejection->wakeUp()->isRequested());

        $rejection->reject('Permissions Violation for Subscription to "_INBOX.JS.PULL.a.*"');
        self::assertSame('Permissions Violation for Subscription to "_INBOX.JS.PULL.a.*"', $rejection->error());
        self::assertTrue($rejection->wakeUp()->isRequested());

        $rejection->reject('maximum subscriptions exceeded');
        self::assertSame('Permissions Violation for Subscription to "_INBOX.JS.PULL.a.*"', $rejection->error(), 'the first rejection is kept');
        self::assertTrue($rejection->wakeUp()->isRequested());
    }

    /**
     * A wait composed with the wake-up ends once another fiber records the rejection, here a timer 10 ms into a 5 s
     * wait, rather than at its end; the error is there when the wait has ended.
     */
    public function testTheWakeUpEndsAWaitOnceTheRejectionIsRecorded(): void
    {
        $rejection = new PullInboxRejection();
        EventLoop::delay(0.01, static function () use ($rejection): void {
            $rejection->reject('maximum subscriptions exceeded');
        });

        $start = hrtime(true);
        try {
            delay(5, cancellation: $rejection->wakeUp());
            self::fail('expected the wake-up to end the wait');
        } catch (CancelledException) {
            // Woken.
        }

        self::assertLessThan(1.0, (hrtime(true) - $start) / 1e9);
        self::assertSame('maximum subscriptions exceeded', $rejection->error());
    }
}
