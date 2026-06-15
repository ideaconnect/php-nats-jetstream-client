<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Exception\NatsException;
use IDCT\NATS\Exception\NatsThrowable;
use IDCT\NATS\Protocol\ProtocolCodec;
use IDCT\NATS\Transport\TlsRequiredException;
use IDCT\NATS\Transport\TransportClosedException;
use PHPUnit\Framework\TestCase;
use ReflectionClassConstant;
use RuntimeException;
use Throwable;

/**
 * Guards #91: every library exception - including the transport exceptions that extend
 * \RuntimeException rather than NatsException - is catchable via the shared NatsThrowable marker, and
 * the CONNECT fallback version is kept current.
 */
final class ExceptionHierarchyTest extends TestCase
{
    /**
     * Widens the static type to Throwable so the instanceof assertions below are runtime checks rather
     * than statically-decidable (always-true/false) ones.
     */
    private function asThrowable(Throwable $throwable): Throwable
    {
        return $throwable;
    }

    public function testNatsExceptionHierarchyImplementsMarker(): void
    {
        self::assertInstanceOf(NatsThrowable::class, $this->asThrowable(new NatsException('x')));
        self::assertInstanceOf(NatsThrowable::class, $this->asThrowable(new ConnectionException('x')));
        self::assertInstanceOf(NatsThrowable::class, $this->asThrowable(new JetStreamException('x')));
    }

    public function testTransportExceptionsImplementMarkerWhileRemainingRuntimeExceptions(): void
    {
        $closed = $this->asThrowable(new TransportClosedException('peer eof'));
        $tls = $this->asThrowable(new TlsRequiredException('no tls context'));

        // Newly catchable as a library error...
        self::assertInstanceOf(NatsThrowable::class, $closed);
        self::assertInstanceOf(NatsThrowable::class, $tls);

        // ...while remaining \RuntimeException/\Throwable so existing catch (\Throwable) paths are
        // unaffected, and deliberately NOT NatsException so catch (NatsException) sites keep their
        // current behavior.
        self::assertInstanceOf(RuntimeException::class, $closed);
        self::assertInstanceOf(RuntimeException::class, $tls);
        self::assertNotInstanceOf(NatsException::class, $closed);
        self::assertNotInstanceOf(NatsException::class, $tls);
    }

    public function testCatchNatsThrowableCatchesATransportException(): void
    {
        $caught = null;

        try {
            throw $this->asThrowable(new TransportClosedException('peer eof'));
        } catch (NatsThrowable $e) {
            $caught = $e;
        }

        // Reaching here means catch (NatsThrowable) handled the transport exception (otherwise it
        // would have propagated out of the test); confirm it is the transport type we threw.
        self::assertInstanceOf(TransportClosedException::class, $caught);
    }

    public function testFallbackClientVersionIsCurrentSemver(): void
    {
        $fallback = (new ReflectionClassConstant(ProtocolCodec::class, 'FALLBACK_CLIENT_VERSION'))->getValue();

        self::assertIsString($fallback);
        self::assertMatchesRegularExpression('/^\d+\.\d+\.\d+$/', $fallback);
        // Regression guard: the source-build fallback must not regress to the stale pre-2.x placeholder.
        self::assertNotSame('1.0.1', $fallback);
    }
}
