<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\JetStream\JetStreamContext;
use IDCT\NATS\Tests\Support\HeldUpDelivery;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\WatchedTransport;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\async;

/**
 * An infinite pull consumer run hands its handler every message it received across a reconnect (#187). The pipelined
 * engine behind PullConsumerIterator::handle() keeps one inbox for the whole run, which the reconnect subscribes again,
 * and ends its pulls in flight once the connection's reconnect count moves, so that it pulls again on the new
 * connection at once rather than at their deadlines (#120). It used to drop those pulls with what they held, and to
 * drop as a straggler any message no pull in flight had room for, while the server, which counted those messages as
 * delivered, sent them again only after the ack wait, or never on a consumer without acks or with max_deliver 1:
 *
 *  - a message the reconnect's own delivery gave the pull in flight, queued behind a delivery a lower sid's handler
 *    held up while another fiber ran the reconnect;
 *  - the answer to a pull the engine issued on the new connection before it came round to see the reconnect, as when
 *    the handler's ack met the dead socket and ran the reconnect inline;
 *  - what a request the server kept across the reconnect brought (a server that outlives the connection serves its
 *    waiting requests once the reconnect subscribes the inbox again), and what a pull written from the reconnect
 *    buffer brought after the run had counted it as lost: more than the pulls the run counts have room for.
 *
 * The run now ends its pulls instead of dropping them: what they received goes to the handler in issue order before
 * any pull goes out, a pull that received nothing leaves without its status being classified, and what no pull has
 * room for goes to the run's overflow, handed over behind the pulls. It checks the reconnect count again after every
 * wait, the handler's included: before each pull it retires, so that a status the old connection brought a later pull
 * neither ends the run nor drops its pin, before it backs off or issues, and before it reads. PullConsumerOverflowTest
 * checks the overflow's place in the run's other paths.
 *
 * Over the scripted server in tests/Support/ReconnectingTransport.php, seen through tests/Support/WatchedTransport.php,
 * which tells the test when the engine reads, with deliveries held up by tests/Support/HeldUpDelivery.php. Where two
 * outcomes compete, the test records which event comes first (the handler getting a message, or a pull going out on
 * the new connection); the time bounds only keep a broken run from hanging the suite, except in the one test that
 * shows the run does not wait out an idle backoff, whose bound sits far from both outcomes. Each test fails on 2.24.2.
 */
final class PullConsumerReconnectTest extends TestCase
{
    use ReconnectScenarios;

    private const PULL_PREFIX = '$JS.API.CONSUMER.MSG.NEXT.S.C';

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /**
     * The pull consumer's own read brings the first pull's message right behind a message for a lower sid whose
     * handler holds the delivery up, and the connection drops while it does: the engine's read is still in that
     * delivery, so the application's read is the one that meets the drop and runs the reconnect. The reconnect's
     * delivery hands the pull's message on (its subscription is not being delivered further up a stack), and the
     * engine's router buffers it into the pull in flight; the hold ends once the new connection is open. The message
     * reaches the handler right after the reconnect, before any pull on the new connection, and the server, which
     * answers only the first pull, is not asked again. The engine used to see the reconnect at the top of its loop
     * and drop the pull in flight with the message in its buffer: it pulled again on the new connection, and the
     * handler never ran; a real server redelivers such a message only after ack_wait.
     */
    public function testAMessageTheReconnectsDeliveryBuffersIntoThePullInFlightReachesTheHandler(): void
    {
        [$transport, , $client, $recorder] = $this->client();
        $hold = new HeldUpDelivery(fallbackSeconds: 5.0);
        $slowSid = $client->subscribe('slow', $hold->handler())->await();
        [$first, $happened] = self::firstOf();
        $server = $this->pullServer($transport, static function (int $pull, string $replyTo, int $sid) use ($transport, $slowSid, $happened): array {
            if ($transport->epoch() >= 1) {
                $happened('a pull on the new connection');
            }

            // Only the first pull is answered: m1 right behind the slow subscription's message, in one chunk.
            return $pull === 1 ? [ReconnectingTransport::msgFrame('slow', $slowSid, 's') . self::messages($sid, 'm1')] : [];
        });

        $handled = self::log();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(4_000);
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $iterator, $happened): void {
            $handled[] = $message->payload;
            $happened('the handler got ' . $message->payload);
            $iterator->stop();
        });
        // The engine's own read is in the slow handler, with m1 queued behind it.
        $hold->began->getFuture()->await(new TimeoutCancellation(3));
        $transport->dropConnection();
        // Nothing reads the socket now: the application's read meets the drop and runs the reconnect.
        async(static fn(): int => $client->processIncoming()->await())->ignore();
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Open && $client->statistics()->reconnects === 1, 4.0);
        self::assertSame([], $handled->getArrayCopy(), 'nothing is handled while the delivery is held up');
        $hold->end('the test');
        $firstEvent = $first->await(new TimeoutCancellation(5));
        $iterator->stop();
        $processed = $run->await(new TimeoutCancellation(5));

        self::assertSame('the handler got m1', $firstEvent, 'the engine dropped the pull holding m1 and pulled again on the new connection');
        self::assertSame(['m1'], $handled->getArrayCopy());
        self::assertSame(1, $processed);
        self::assertSame([0], $server->epochs, 'the server, which answers only the first pull, is not asked again');
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Disconnected, ConnectionEvent::Reconnected], $recorder->events);
    }

    /**
     * The run ends a pull that received nothing without holding back the pull behind it: depth 2, and the chunk that
     * answers the second pull of the first generation brings the slow subscription's message, a 408 Request Timeout on
     * the first pull's reply subject, then m1, which the router puts into the second pull. The engine's read is held
     * up in the slow handler with the rest queued behind it, the connection drops, and the application's read runs
     * the reconnect, whose delivery hands the queued frames on. Once the hold ends, the run ends both pulls: the first,
     * empty, leaves the run without its 408 being classified, and the second's m1 reaches the handler before any pull
     * goes out on the new connection; the server saw exactly two pulls, both on the first connection. A run that
     * stopped retiring at the empty pull would issue a pull on the new connection first. The engine used to drop both
     * pulls with m1 and pull again.
     */
    public function testAnEndedPullWithoutMessagesDoesNotHoldBackTheMessageOfTheNext(): void
    {
        [$transport, , $client] = $this->client();
        $hold = new HeldUpDelivery(fallbackSeconds: 5.0);
        $slowSid = $client->subscribe('slow', $hold->handler())->await();
        [$first, $happened] = self::firstOf();
        $firstReplyTo = null;
        $server = $this->pullServer($transport, static function (int $pull, string $replyTo, int $sid) use ($transport, $slowSid, $happened, &$firstReplyTo): array {
            if ($transport->epoch() >= 1) {
                $happened('a pull on the new connection');
            }
            $firstReplyTo ??= $replyTo;
            if ($pull !== 2) {
                return [];
            }

            return [
                ReconnectingTransport::msgFrame('slow', $slowSid, 's')
                . self::statusFrame($firstReplyTo, $sid, 408, 'Request Timeout')
                . self::messages($sid, 'm1'),
            ];
        });

        $handled = self::log();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(2)->setExpiresMs(4_000);
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $iterator, $happened): void {
            $handled[] = $message->payload;
            $happened('the handler got ' . $message->payload);
            $iterator->stop();
        });
        $hold->began->getFuture()->await(new TimeoutCancellation(3));
        $transport->dropConnection();
        async(static fn(): int => $client->processIncoming()->await())->ignore();
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Open && $client->statistics()->reconnects === 1, 4.0);
        $hold->end('the test');
        $firstEvent = $first->await(new TimeoutCancellation(5));
        $iterator->stop();
        $processed = $run->await(new TimeoutCancellation(5));

        self::assertSame('the handler got m1', $firstEvent, 'the ended pull without messages held back the next one');
        self::assertSame(['m1'], $handled->getArrayCopy());
        self::assertSame(1, $processed);
        self::assertSame([0, 0], $server->epochs, 'two pulls, both on the first connection');
    }

    /**
     * The iterator's drain() keeps its promise across a reconnect: the reproduction's shape, the handler not stopping,
     * and the iterator drained once the hold began. The run hands m1, which the reconnect's delivery gave its pull,
     * to the handler, issues nothing more, and resolves with 1 after one pull. The run used to resolve with 0, the
     * handler never having run, although drain() lets every message already fetched reach the handler.
     */
    public function testADrainAcrossTheReconnectStillDeliversWhatThePullInFlightReceived(): void
    {
        [$transport, , $client] = $this->client();
        $hold = new HeldUpDelivery(fallbackSeconds: 5.0);
        $slowSid = $client->subscribe('slow', $hold->handler())->await();
        $server = $this->pullServer($transport, static fn(int $pull, string $replyTo, int $sid): array => $pull === 1
            ? [ReconnectingTransport::msgFrame('slow', $slowSid, 's') . self::messages($sid, 'm1')]
            : []);

        $handled = self::log();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(4_000);
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled): void {
            $handled[] = $message->payload;
        });
        $hold->began->getFuture()->await(new TimeoutCancellation(3));
        $iterator->drain();
        $transport->dropConnection();
        async(static fn(): int => $client->processIncoming()->await())->ignore();
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Open && $client->statistics()->reconnects === 1, 4.0);
        $hold->end('the test');
        $processed = $run->await(new TimeoutCancellation(5));

        self::assertSame(1, $processed);
        self::assertSame(['m1'], $handled->getArrayCopy());
        self::assertSame([0], $server->epochs, 'one pull: the drained run issues nothing more');
    }

    /**
     * A reconnect that the handler runs is seen before the run pulls again: no held delivery, the server answers the
     * first pull with m1 and the second with m2, and the handler of m1 drops the connection and acks, so that the ack's
     * failed write runs the reconnect inline. The run sees the reconnect before it issues its next pull, which goes out
     * on the new connection under the new count, and m2, its answer, reaches the handler before any third pull: two
     * pulls, one on each connection. The engine used to issue that pull before it saw the reconnect, then drop it with
     * m2 at the top of its loop and pull a third time.
     */
    public function testAPullIssuedWhileTheHandlerRanTheReconnectKeepsItsMessage(): void
    {
        [$transport, , $client] = $this->client();
        [$first, $happened] = self::firstOf();
        $server = $this->pullServer($transport, static function (int $pull, string $replyTo, int $sid) use ($happened): array {
            return match ($pull) {
                1 => [self::messages($sid, 'm1')],
                2 => [self::messages($sid, 'm2')],
                default => (static function () use ($happened): array {
                    $happened('a third pull');

                    return [];
                })(),
            };
        });

        $handled = self::log();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(30_000);
        $run = $iterator->handle(static function (NatsMessage $message, JetStreamContext $js) use ($handled, $transport, $iterator, $happened): void {
            $handled[] = $message->payload;
            if ($message->payload === 'm1') {
                $transport->dropConnection();
                $js->ack($message)->await();

                return;
            }
            $happened('the handler got ' . $message->payload);
            $iterator->stop();
        });
        $firstEvent = $first->await(new TimeoutCancellation(5));
        $iterator->stop();
        $processed = $run->await(new TimeoutCancellation(5));

        self::assertSame('the handler got m2', $firstEvent, 'the pull issued on the new connection was dropped with m2');
        self::assertSame(['m1', 'm2'], $handled->getArrayCopy());
        self::assertSame(2, $processed);
        self::assertSame([0, 1], $server->epochs, 'one pull on each connection');
        self::assertSame(1, $client->statistics()->reconnects);
    }

    /**
     * A pull issued after a reconnect the handler ran is not ended with the old ones: batch 2, the first pull is
     * answered with m1 and a 409 Batch Completed on its reply subject, and m1's handler runs the reconnect through its
     * ack, as above. The second pull, on the new connection, is answered with m2 only, and the server sends m3 for it
     * once the engine's next read takes the socket. The run keeps that pull, which it issued under the new count, in
     * flight until m3 fills it: m2 and m3 reach the handler before any third pull. Issued under the old count, the
     * pull was ended at the top of the loop with m2, and a third pull went out before m3 came. The engine used to
     * drop that pull with m2.
     */
    public function testAPullIssuedAfterAReconnectInTheHandlerIsNotEndedWithTheOldOnes(): void
    {
        [$transport, $watched, $client] = $this->client();
        [$first, $happened] = self::firstOf();
        $server = $this->pullServer($transport, static function (int $pull, string $replyTo, int $sid) use ($transport, $watched, $happened): array {
            if ($pull === 1) {
                return [self::messages($sid, 'm1') . self::statusFrame($replyTo, $sid, 409, 'Batch Completed')];
            }
            if ($pull === 2) {
                // m3 comes once the engine's next read takes the socket.
                $watched->onRead = static function () use ($watched, $transport, $sid): void {
                    $watched->onRead = null;
                    $transport->pushFrame(self::messages($sid, 'm3'));
                };

                return [self::messages($sid, 'm2')];
            }
            $happened('a third pull');

            return [];
        });

        $handled = self::log();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(2)->setDepth(1)->setExpiresMs(30_000);
        $run = $iterator->handle(static function (NatsMessage $message, JetStreamContext $js) use ($handled, $transport, $iterator, $happened): void {
            $handled[] = $message->payload;
            if ($message->payload === 'm1') {
                $transport->dropConnection();
                $js->ack($message)->await();
            } elseif ($message->payload === 'm3') {
                $happened('the handler got m3');
                $iterator->stop();
            }
        });
        $firstEvent = $first->await(new TimeoutCancellation(5));
        $iterator->stop();
        $processed = $run->await(new TimeoutCancellation(5));

        self::assertSame('the handler got m3', $firstEvent, 'a third pull went out before m3 was handled');
        self::assertSame(['m1', 'm2', 'm3'], $handled->getArrayCopy());
        self::assertSame(3, $processed);
        self::assertSame([0, 1], $server->epochs);
    }

    /**
     * A reconnect the handler of one pull runs is seen before the next pull's status is classified: batch 1, depth 2,
     * and the second pull's answer brings m-1, which goes to the first pull, the oldest open, and a terminal 409
     * Consumer Deleted for the second pull itself. The handler of m-1 runs the reconnect through its ack. The run comes
     * round to the top of its loop before it retires the second pull, which it ends with the old connection: the 409
     * neither ends the run nor reaches onError, and the run pulls twice on the new connection, whose answer, m-2,
     * reaches the handler. Retired first, the old 409 ended the run through onError with one message processed, and so
     * did the engine, which used to drop the pulls at the top of its loop only after the retire phase.
     */
    public function testAReconnectInTheHandlerIsSeenBeforeTheNextPullsStatusIsClassified(): void
    {
        $errors = self::log();
        [$transport, , $client] = $this->client();
        $server = $this->pullServer($transport, static fn(int $pull, string $replyTo, int $sid): array => match ($pull) {
            2 => [self::messages($sid, 'm-1') . self::statusFrame($replyTo, $sid, 409, 'Consumer Deleted')],
            3 => [self::messages($sid, 'm-2')],
            default => [],
        });

        $handled = self::log();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(2)->setExpiresMs(30_000)
            ->setOnError(static function (\Throwable $error) use ($errors): void {
                $errors[] = $error->getMessage();
            });
        $processed = $iterator->handle(static function (NatsMessage $message, JetStreamContext $js) use ($handled, $transport, $iterator): void {
            $handled[] = $message->payload;
            if ($message->payload === 'm-1') {
                $transport->dropConnection();
                $js->ack($message)->await();
            } else {
                $iterator->stop();
            }
        })->await(new TimeoutCancellation(5));

        self::assertSame(['m-1', 'm-2'], $handled->getArrayCopy());
        self::assertSame([], $errors->getArrayCopy(), 'the old connection\'s 409 does not reach onError');
        self::assertSame(2, $processed);
        self::assertSame([0, 0, 1, 1], $server->epochs);
        self::assertSame(1, $client->statistics()->reconnects);
    }

    /**
     * The same rule keeps a pinned group's pin: a stale-pin 423 the old connection brought a pull behind the one whose
     * handler runs the reconnect leaves the run unclassified. The group's first pull captures the pin from m-1, the run
     * fans out to two pulls carrying it, and the answer to the second of them brings m-2 for the first and a 423 for
     * itself; m-2's handler runs the reconnect through its ack. The pulls the run issues on the new connection still
     * carry the pin, and m-3 reaches the handler. Classified, the 423 dropped the pin, and the next pull went out
     * without it, serially, to capture a pin again.
     */
    public function testAStalePinStatusTheOldConnectionBroughtDoesNotDropThePin(): void
    {
        [$transport, , $client] = $this->client();
        $pins = [];
        $server = $this->pullServer($transport, static function (int $pull, string $replyTo, int $sid, string $payload) use ($transport, &$pins): array {
            /** @var array<string, mixed> $request */
            $request = json_decode($payload, true, flags: JSON_THROW_ON_ERROR);
            $pins[] = [$transport->epoch(), $request['id'] ?? null];

            return match ($pull) {
                1 => [self::pinnedMessage($sid, 'm-1', 'pin-1')],
                3 => [self::pinnedMessage($sid, 'm-2', 'pin-1') . self::statusFrame($replyTo, $sid, 423, 'Nats-Pin-Id mismatch')],
                4 => [self::pinnedMessage($sid, 'm-3', 'pin-1')],
                default => [],
            };
        });

        $handled = self::log();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(2)->setExpiresMs(30_000)
            ->setGroup('workers');
        $processed = $iterator->handle(static function (NatsMessage $message, JetStreamContext $js) use ($handled, $transport, $iterator): void {
            $handled[] = $message->payload;
            if ($message->payload === 'm-2') {
                $transport->dropConnection();
                $js->ack($message)->await();
            } elseif ($message->payload === 'm-3') {
                $iterator->stop();
            }
        })->await(new TimeoutCancellation(5));

        self::assertSame(['m-1', 'm-2', 'm-3'], $handled->getArrayCopy());
        self::assertSame(3, $processed);
        self::assertSame([0, 0, 0, 1, 1], $server->epochs);
        self::assertSame([[0, null], [0, 'pin-1'], [0, 'pin-1'], [1, 'pin-1'], [1, 'pin-1']], $pins, 'the pulls on the new connection still carry the pin');
    }

    /**
     * What a request the server kept across the reconnect brings reaches the handler: batch 1, depth 1, the server
     * holds the first pull, and the connection drops with no message; the server stays up, so once the reconnect
     * subscribes the run's inbox again it still holds that request besides the one the run issues on the new
     * connection, and answers both together: m-1 and m-2. The run's one pull in flight has room for m-1; m-2 goes to
     * the run's overflow and reaches the handler right behind it, and the drained run returns 2. Both pulls used the
     * one inbox the reconnect replayed. The router used to drop m-2 as a straggler, which nats-server 2.12 showed lost
     * on a consumer with ack_policy none: delivered, acked by policy, never handled.
     */
    public function testWhatARequestTheServerKeptAcrossTheReconnectBringsReachesTheHandler(): void
    {
        [$transport, $watched, $client] = $this->client();
        $server = $this->pullServer($transport, static fn(int $pull, string $replyTo, int $sid): array => $pull === 2
            ? [self::messages($sid, 'm-1', 'm-2')]
            : []);

        $handled = self::log();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(10_000);
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $iterator): void {
            $handled[] = $message->payload;
            // The iterator's drain() lets what was received reach the handler and issues nothing more.
            $iterator->drain();
        });
        $this->waitUntil(static fn(): bool => $server->epochs !== [] && $watched->readsUnderWay === 1);
        $transport->dropConnection();
        $processed = $run->await(new TimeoutCancellation(5));

        self::assertSame(['m-1', 'm-2'], $handled->getArrayCopy());
        self::assertSame(2, $processed);
        self::assertSame([0, 1], $server->epochs);
        self::assertSame(1, $client->statistics()->reconnects);
        self::assertCount(1, array_unique(array_map(self::inboxOf(...), $server->replyTos)), 'one inbox for the whole run');
    }

    /**
     * What a pull written from the reconnect buffer brings reaches the handler: batch 2, depth 1, and a pull that
     * expires at once, so that its deadline passes while the reconnect's dial is held, and the run replaces it while
     * the connection is still Connecting: the replacement's PUB goes into the reconnect buffer, which the test sees
     * before it lets the dial through. The reconnect writes that PUB on the new connection, where the run, seeing the
     * reconnect, ends it and pulls again: the server answers both requests together, m-1 and m-2 for the buffered
     * one and m-3 and m-4 for the run's own. The run's one pull has room for two; the other two go to the overflow,
     * and all four reach the handler in order. The router used to drop m-3 and m-4 as stragglers.
     */
    public function testWhatAPullWrittenFromTheReconnectBufferBringsReachesTheHandler(): void
    {
        [$transport, $watched, $client] = $this->client();
        $server = $this->pullServer($transport, static fn(int $pull, string $replyTo, int $sid): array => $pull === 3
            ? [self::messages($sid, 'm-1', 'm-2', 'm-3', 'm-4')]
            : []);

        $handled = self::log();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(2)->setDepth(1)->setExpiresMs(1);
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $iterator): void {
            $handled[] = $message->payload;
            $iterator->drain();
        });
        $this->waitUntil(static fn(): bool => $server->epochs !== [] && $watched->readsUnderWay === 1);
        $transport->holdNextDial();
        $transport->dropConnection();
        // The first pull's deadline passes while the dial is held: the run replaces it, into the reconnect buffer.
        $this->waitUntil(static fn(): bool => self::reconnectBufferOf($client) !== '', 4.0);
        $transport->releaseDial();
        $processed = $run->await(new TimeoutCancellation(5));

        self::assertSame(['m-1', 'm-2', 'm-3', 'm-4'], $handled->getArrayCopy());
        self::assertSame(4, $processed);
        self::assertSame([0, 1, 1], $server->epochs, 'the buffered replacement and the run\'s own pull both went out on the new connection');
    }

    /**
     * A pull whose own write ran the reconnect is replaced, and what its request brings still reaches the handler:
     * batch 2, depth 1, and the first pull's write finds the socket dead, so that the write runs the reconnect inline
     * and writes the pull again on the new connection. The run cannot tell such a write from one the old connection
     * took, so it ends that pull with the old count and pulls again; the server answers both requests together, four
     * messages, and all four reach the handler in order and count in the drained run's result: the pull's two, and two
     * from the overflow. The engine used to read on for that pull until its deadline, then drop the two messages its
     * second pull had no room for.
     */
    public function testAPullWhoseOwnWriteRanTheReconnectIsReplacedAndWhatItsRequestBringsStillReachesTheHandler(): void
    {
        [$transport, , $client] = $this->client();
        $transport->failNextWriteContaining(self::PULL_PREFIX);
        $server = $this->pullServer($transport, static fn(int $pull, string $replyTo, int $sid): array => $pull === 2
            ? [self::messages($sid, 'm-1', 'm-2', 'm-3', 'm-4')]
            : []);

        $handled = self::log();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(2)->setDepth(1)->setExpiresMs(1_000);
        $processed = $iterator->handle(static function (NatsMessage $message) use ($handled, $iterator): void {
            $handled[] = $message->payload;
            $iterator->drain();
        })->await(new TimeoutCancellation(8));

        self::assertSame(['m-1', 'm-2', 'm-3', 'm-4'], $handled->getArrayCopy());
        self::assertSame(4, $processed);
        self::assertSame([1, 1], $server->epochs, 'the write that ran the reconnect and the replacement both went out on the new connection');
    }

    /**
     * A pull whose own write ran the reconnect is the last the issue phase writes under the old count: depth 2, and
     * the first pull's write runs the reconnect inline, as above. The run issues no second pull under the old count;
     * it ends the first and issues a fresh generation of two, so that three pulls reach the new connection before the
     * run reads. The server answers them together with three messages, m-1 for the ended pull's request and one for
     * each of the others: the two pulls take one each, the overflow the third, and all three reach the handler in
     * order. Issued under the old count, the second pull was ended with the first, and the run wrote four pulls. The
     * engine used to read on for those pulls until their deadline, and drop the message no pull had room for.
     */
    public function testAPullWhoseOwnWriteRanTheReconnectIsTheLastIssuedUnderTheOldCount(): void
    {
        [$transport, $watched, $client] = $this->client();
        $transport->failNextWriteContaining(self::PULL_PREFIX);
        $server = $this->pullServer($transport, static fn(): array => []);

        $handled = self::log();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(2)->setExpiresMs(30_000);
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $iterator): void {
            $handled[] = $message->payload;
            $iterator->drain();
        });
        $this->waitUntil(static fn(): bool => count($server->epochs) >= 3 && $watched->readsUnderWay === 1, 4.0);
        $pullsBeforeTheRead = $server->epochs;
        $transport->pushFrame(self::messages((int) $transport->sidFor($server->replyTos[0]), 'm-1', 'm-2', 'm-3'));
        $processed = $run->await(new TimeoutCancellation(5));

        self::assertSame([1, 1, 1], $pullsBeforeTheRead, 'the ended pull and a fresh generation of two, all on the new connection');
        self::assertSame(['m-1', 'm-2', 'm-3'], $handled->getArrayCopy());
        self::assertSame(3, $processed);
    }

    /**
     * A reconnect that onError runs is seen before the idle backoff: no_wait, depth 1, the stream empty for six pulls
     * (404 No Messages each), so that the backoff between them has grown to 320 ms, and the seventh pull is answered
     * with a 503 No Responders, whose onError, the first of that outage, publishes an alert on the dead connection,
     * which runs the reconnect inline. The run sees the reconnect in the retire phase, right after onError, and pulls on
     * the new connection at once, the reset clearing the idle state, before a timer of 250 ms that starts when onError
     * returns fires; waiting out the backoff first held that pull for 500 ms, and so did the engine, which used to see the
     * reconnect only at the top of its loop, after the backoff. The bound sits far from both outcomes.
     */
    public function testAReconnectThatOnErrorRanIsSeenBeforeTheIdleBackoff(): void
    {
        [$transport, , $client] = $this->client();
        [$first, $happened] = self::firstOf();
        $server = $this->pullServer($transport, static function (int $pull, string $replyTo, int $sid) use ($transport, $happened): array {
            if ($transport->epoch() >= 1) {
                $happened('a pull on the new connection');

                return [self::messages($sid, 'm-1')];
            }

            return [self::statusFrame($replyTo, $sid, $pull < 7 ? 404 : 503, $pull < 7 ? 'No Messages' : 'No Responders')];
        });

        $handled = self::log();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(30_000)->setNoWait(true);
        $iterator->setOnError(static function () use ($transport, $client, $happened): void {
            $transport->dropConnection();
            $client->publish('alerts', 'no responders')->await();
            EventLoop::delay(0.25, static fn() => $happened('the timer'));
        });
        $processed = $iterator->handle(static function (NatsMessage $message) use ($handled, $iterator): void {
            $handled[] = $message->payload;
            $iterator->stop();
        })->await(new TimeoutCancellation(8));

        self::assertSame('a pull on the new connection', $first->await(new TimeoutCancellation(1)), 'the run waited out the idle backoff before it saw the reconnect');
        self::assertSame(['m-1'], $handled->getArrayCopy());
        self::assertSame(1, $processed);
        self::assertSame([0, 0, 0, 0, 0, 0, 0, 1], $server->epochs);
    }

    /**
     * A connected client over a watched ReconnectingTransport, closed in tearDown, with no heartbeat, so that only the
     * run and the test read: reconnect on, with waiting for it enabled and a 5 to 20 ms backoff.
     *
     * @return array{ReconnectingTransport, WatchedTransport, NatsClient, LifecycleRecorder}
     */
    private function client(): array
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $client = $this->own(new NatsClient(new NatsOptions(
            connectTimeoutMs: 500,
            requestTimeoutMs: 2_000,
            reconnectEnabled: true,
            maxReconnectAttempts: 1_000,
            reconnectDelayMs: 5,
            reconnectMaxDelayMs: 20,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
            connectionListener: $recorder->connectionListener(),
            errorListener: $recorder->errorListener(),
        ), $watched));
        $this->opened[] = $client;
        $client->connect()->await();

        return [$transport, $watched, $client, $recorder];
    }

    /**
     * The scripted server's side of the run's pulls: records the epoch (session) and the reply subject of every pull
     * written to a live session, in order, and answers each with what $answer returns, given the pull's number (1 for
     * the first), its reply subject, the sid of the run's inbox on that session and the pull's payload; with no frames,
     * the server holds the pull, as one with no message does until it expires.
     *
     * @param \Closure(int, string, int, string): list<string> $answer
     * @return object{epochs: list<int>, replyTos: list<string>}
     */
    private function pullServer(ReconnectingTransport $transport, \Closure $answer): object
    {
        $server = new class {
            /** @var list<int> The epoch of each pull, in order. */
            public array $epochs = [];
            /** @var list<string> The reply subject of each pull, in order. */
            public array $replyTos = [];
        };
        $transport->responder = static function (string $subject, ?string $replyTo, string $payload) use ($transport, $server, $answer): array {
            if ($replyTo === null || $subject !== self::PULL_PREFIX) {
                return [];
            }

            $server->epochs[] = $transport->epoch();
            $server->replyTos[] = $replyTo;
            $sid = $transport->sidFor($replyTo);
            self::assertNotNull($sid, 'the run\'s inbox is subscribed on the session its pull went out on');

            return $answer(count($server->epochs), $replyTo, $sid, $payload);
        };

        return $server;
    }

    /**
     * Which of the events a test records happens first: the future completes with the first, later ones are ignored.
     *
     * @return array{Future<string>, \Closure(string): void}
     */
    private static function firstOf(): array
    {
        /** @var DeferredFuture<string> $first */
        $first = new DeferredFuture();

        return [$first->getFuture(), static function (string $event) use ($first): void {
            if (!$first->isComplete()) {
                $first->complete($event);
            }
        }];
    }

    /** One chunk of messages on the run's inbox, each as a real server delivers a pull's message, with an ack subject. */
    private static function messages(int $sid, string ...$payloads): string
    {
        $chunk = '';
        $sequence = 0;
        foreach ($payloads as $payload) {
            ++$sequence;
            $chunk .= ReconnectingTransport::msgFrame('evt.s', $sid, $payload, sprintf('$JS.ACK.S.C.1.%d.%d.0.0', $sequence, $sequence));
        }

        return $chunk;
    }

    /** A message of a pinned_client group, carrying the pin the server assigned the run. */
    private static function pinnedMessage(int $sid, string $payload, string $pin): string
    {
        return ReconnectingTransport::hmsgFrame('evt.s', $sid, "NATS/1.0\r\nNats-Pin-Id: " . $pin . "\r\n\r\n", $payload, '$JS.ACK.S.C.1.1.1.0.0');
    }

    /** A pull status frame, on the pull's reply subject as the server sends it. */
    private static function statusFrame(string $replyTo, int $sid, int $code, string $description): string
    {
        return ReconnectingTransport::hmsgFrame($replyTo, $sid, sprintf("NATS/1.0 %d %s\r\n\r\n", $code, $description), '');
    }

    /** The run's inbox a pull's reply subject belongs to: the subject without its token. */
    private static function inboxOf(string $replyTo): string
    {
        return substr($replyTo, 0, (int) strrpos($replyTo, '.'));
    }

    /** What the client's connection holds in its reconnect buffer: publishes made while a reconnect is under way. */
    private static function reconnectBufferOf(NatsClient $client): string
    {
        $connection = (new \ReflectionProperty(NatsClient::class, 'connection'))->getValue($client);
        self::assertInstanceOf(NatsConnection::class, $connection);
        $buffer = (new \ReflectionProperty(NatsConnection::class, 'reconnectBuffer'))->getValue($connection);
        self::assertIsString($buffer);

        return $buffer;
    }

    /** @return \ArrayObject<int, string> What a handler, or onError, logged, in order. */
    private static function log(): \ArrayObject
    {
        return new \ArrayObject();
    }
}
