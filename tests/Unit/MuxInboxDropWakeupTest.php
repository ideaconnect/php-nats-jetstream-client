<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\HeldUpDelivery;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\SubscriptionLimitServer;
use IDCT\NATS\Tests\Support\WatchedTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\async;
use function Amp\delay;

/**
 * The wake-up of a request's read when the reply inbox is dropped or rejected (#180). request() and requestMany() share
 * one reply inbox, and the client drops it inline in the dispatch of the -ERR that rejects it: 'maximum subscriptions
 * exceeded' before the server is known to hold it, or a permissions violation naming it (#167). A request notices at
 * its next look, and since #174 its read is woken by its own reply alone: when ANOTHER fiber's read brought such an
 * -ERR while the request's read was parked on the socket, nothing woke it, and the request noticed only with the
 * server's next bytes or at its deadline. The shape arises when that fiber's dispatch is held up ahead of the -ERR, as
 * it is while it awaits the write of the PONG for a server PING in the same chunk, under socket backpressure: the read
 * slot is free by then, the request's read takes it and waits, the PONG's write completes, and the -ERR drops the
 * inbox under a read nothing ends. The same when the request is parked at its own wait for the read slot, another
 * operation's read holding it: nothing ended that wait either. The connection now fires every waiting request's
 * wake-up when it drops or rejects the inbox, every wait of a request ends with it, and the request fails at once with
 * the error it would have found at its next look, or returns a reply delivered by then. A terminal close that follows
 * the rejection at once, an error listener's disconnect(), resets the rejection latch: the request then ends with the
 * closed connection at its next look, where a woken request would otherwise look on until its deadline.
 *
 * Over `tests/Support/ReconnectingTransport.php` seen through `tests/Support/WatchedTransport.php`, whose onRead tells
 * the test when the operation's read has taken the socket: the PONG's write is held up until then and released by the
 * test, so the order of events decides, not time. Each regression test fails on the code before the fix, the request
 * ending only at its 3 s timeout; the guards pass on both.
 */
final class MuxInboxDropWakeupTest extends TestCase
{
    use ReconnectScenarios;

    private const MUX_DROPPED = 'the server may have rejected the shared reply-inbox subscription';
    private const MUX_REJECTED = 'was rejected by the server (permissions violation)';

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /** @return iterable<string, array{string, string}> */
    public static function requestsAndRejections(): iterable
    {
        foreach (['request()' => 'request', 'requestMany(max 1)' => 'requestMany'] as $name => $operation) {
            yield $name . ', the subscription limit' => [$operation, 'limit'];
            yield $name . ', permissions' => [$operation, 'permissions'];
        }
    }

    /**
     * The main case. The application's read loop holds the socket; a request on an inbox the server has not confirmed
     * is in flight, parked behind that read; the server's next chunk holds a PING and, behind it, the -ERR that drops
     * the inbox ('maximum subscriptions exceeded', which drops an unconfirmed inbox) or rejects it (a permissions
     * violation naming it). The PONG's write is held up until the request's read has taken the socket, then released:
     * the dispatch meets the -ERR with the request's read parked on the socket. The request fails within 0.5 s of its
     * 3 s timeout with the dropped-inbox error, or the permissions error, and leaves no wake-up behind. After the drop
     * the next request subscribes a new inbox, which the server now confirms, and gets its reply; after the rejection
     * the next request fails fast without subscribing again, as #167 documents. The request used to end only at its
     * deadline, with the same error: the deadline woke its read, and the top of its loop found the drop before it.
     */
    #[DataProvider('requestsAndRejections')]
    public function testARequestParkedOnTheSocketFailsAsSoonAsAnotherFibersReadDropsTheInbox(string $operation, string $rejection): void
    {
        [$transport, $watched, $client] = $this->connectForTheDrop();
        [$stop, $loop] = $this->startReadLoop($client);
        $this->awaitRead($watched);

        try {
            $start = hrtime(true);
            $result = self::startRequest($operation, $client, 3_000);
            $replyTo = $this->awaitPublished($transport);
            $this->dropTheInboxWhileTheOperationReads($transport, $watched, self::errFrame($rejection, self::inboxOf($replyTo)));
            [, $error] = self::settle($result);
            $elapsed = $this->secondsSince($start);

            self::assertInstanceOf(ConnectionException::class, $error, sprintf('%s ended after %.3f s with %s', $operation, $elapsed, self::describe($error)));
            self::assertStringContainsString($rejection === 'limit' ? self::MUX_DROPPED : self::MUX_REJECTED, $error->getMessage());
            self::assertLessThan(0.5, $elapsed, sprintf('%s failed after %.3f s, with a 3 s timeout', $operation, $elapsed));
            self::assertSame([], self::muxWakes(self::connectionOf($client)), 'no wake-up is left behind for the request that failed');

            $start = hrtime(true);
            if ($rejection === 'limit') {
                // The server answers the PING behind the new inbox's SUB, confirming it.
                $transport->answerPings = true;
                self::assertSame('ok', $client->request('svc.echo', 'y', 2_000)->await()->payload);
                self::assertCount(2, $transport->controlLinesStartingWith('SUB _INBOX.'), 'the next request subscribed a new inbox');
            } else {
                try {
                    $client->request('svc.echo', 'y', 3_000)->await();
                    self::fail('expected the next request to fail: request/reply is unavailable until the connection closes for good');
                } catch (ConnectionException $e) {
                    self::assertStringContainsString(self::MUX_REJECTED, $e->getMessage());
                }
                self::assertLessThan(0.5, $this->secondsSince($start), 'the next request failed fast');
                self::assertCount(1, $transport->controlLinesStartingWith('SUB _INBOX.'), 'the latch kept the next request from subscribing again');
            }
        } finally {
            $stop->cancel();
            $loop->await();
        }
    }

    /**
     * A requestMany() that has collected a reply, read by the application's loop and delivered behind a handler that
     * held that delivery up, reads on for the next one behind the application's read. The permissions -ERR that rejects
     * the inbox then comes the same way: the request returns what it has at once, within 0.5 s of its 3 s timeout,
     * rather than at its timeout. Only the permissions rejection can be staged here: the reply confirmed the inbox, so a
     * 'maximum subscriptions exceeded' leaves it alone.
     */
    public function testARequestManyWithRepliesCollectedReturnsThemAsSoonAsAnotherFibersReadRejectsTheInbox(): void
    {
        [$transport, $watched, $client] = $this->connectForTheDrop();
        $hold = new HeldUpDelivery();
        // Subscribed ahead of the reply inbox, so its sid is the lower one: the inbox's delivery waits behind its handler.
        $slowSid = $client->subscribe('slow', $hold->handler())->await();
        [$stop, $loop] = $this->startReadLoop($client);
        $this->awaitRead($watched);

        try {
            $many = $client->requestMany('svc.held', 'x', null, 2, 3_000)->map(self::payloads(...));
            $many->ignore();
            $replyTo = $this->awaitPublished($transport);
            $inboxSid = (int) $transport->sidFor($replyTo);
            // The reply is queued behind the held-up delivery; the request's read takes the socket meanwhile and ends
            // the hold-up, the reply is delivered as the handler returns, and the request's read ends with it.
            $transport->pushFrame(ReconnectingTransport::msgFrame('slow', $slowSid, 's') . ReconnectingTransport::msgFrame($replyTo, $inboxSid, 'first'));
            $hold->began->getFuture()->await(new TimeoutCancellation(2));
            $watched->onRead = $hold->endWhenARead();
            $hold->returned->getFuture()->await(new TimeoutCancellation(2));
            self::assertSame('a read taking the socket', $hold->endedBy, "the request's read took the socket during the hold-up");
            // The request pauses after that idle read, and the application's loop takes the socket back: the request,
            // with one reply and room for another, then reads on behind it.
            $readsAfterTheReply = $watched->reads;
            $this->waitUntil(static fn(): bool => $watched->reads > $readsAfterTheReply && $watched->readsUnderWay === 1);
            self::assertFalse($many->isComplete());

            $start = hrtime(true);
            $this->dropTheInboxWhileTheOperationReads($transport, $watched, self::errFrame('permissions', self::inboxOf($replyTo)));
            [$payloads, $error] = self::settle($many);
            $elapsed = $this->secondsSince($start);

            self::assertNull($error, sprintf('requestMany() ended after %.3f s with %s', $elapsed, self::describe($error)));
            self::assertSame(['first'], $payloads);
            self::assertLessThan(0.5, $elapsed, sprintf('requestMany() returned after %.3f s, with a 3 s timeout', $elapsed));
        } finally {
            $stop->cancel();
            $loop->await();
        }
    }

    /** @return iterable<string, array{string, string}> */
    public static function slotHoldersAndOperations(): iterable
    {
        foreach (['a poll' => 'poll', 'a flush' => 'flush'] as $name => $holder) {
            yield $name . ' holds the socket, request()' => [$holder, 'request'];
            yield $name . ' holds the socket, requestMany(max 1)' => [$holder, 'requestMany'];
        }
    }

    /**
     * The request waits at its own wait for the read slot, not in a read: another operation's read - a
     * SubscriptionQueue poll's, a flush's - holds the socket when the request looks, so it parks on its reply or on the
     * slot's release, with its budget, without reading. The application's loop read the chunk with the PING and the
     * -ERR before that read took the socket, and its dispatch is held up at the PONG's write while the request is
     * issued. The write is released, the -ERR drops the inbox, and the request fails at once with the dropped-inbox
     * error, within 0.5 s of its 3 s timeout, having never read the socket. It used to notice only once the other
     * operation's read ended, here at that operation's 2 s timeout: the drop ended a request's read, not this wait.
     */
    #[DataProvider('slotHoldersAndOperations')]
    public function testARequestParkedAtItsOwnWaitBehindAnotherOperationsReadFailsAsSoonAsTheInboxIsDropped(string $holder, string $operation): void
    {
        [$transport, $watched, $client] = $this->connectForTheDrop();
        $queue = $client->subscribeQueue('jobs')->await();
        [$stop, $loop] = $this->startReadLoop($client);
        $this->awaitRead($watched);

        try {
            // The application's read brings the PING and the -ERR, and its dispatch is held up at the PONG's write,
            // with the read slot free.
            $readsBefore = $watched->reads;
            $transport->stallNextWriteContaining('PONG', 5.0);
            $transport->pushFrame("PING\r\n-ERR 'maximum subscriptions exceeded'\r\n");
            $this->waitUntil(static fn(): bool => $transport->writesStalled() > 0 && $watched->readsUnderWay === 0);

            // The other operation's read takes the socket meanwhile: the flush's for the PONG of its own PING, which
            // the server, answering no PINGs here, does not send.
            /** @var Future<mixed> $holding */
            $holding = $holder === 'poll'
                ? async(static fn(): ?NatsMessage => $queue->setTimeout(2.0)->next())
                : $client->flush();
            $holding->ignore();
            $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1 && $watched->reads === $readsBefore + 1);

            // The request is sent on an inbox the server has not confirmed, and parks at its own wait.
            $start = hrtime(true);
            $result = self::startRequest($operation, $client, 3_000);
            $this->awaitPublished($transport);
            $connection = self::connectionOf($client);
            self::assertCount(1, self::muxWakes($connection), 'the request registered its wake-up');
            self::assertSame($readsBefore + 1, $watched->reads, "the request's read did not reach the socket: the other operation's read holds it");

            $transport->releaseStalledWrites();
            [, $error] = self::settle($result);
            $elapsed = $this->secondsSince($start);

            self::assertInstanceOf(ConnectionException::class, $error, sprintf('%s ended after %.3f s with %s', $operation, $elapsed, self::describe($error)));
            self::assertStringContainsString(self::MUX_DROPPED, $error->getMessage());
            self::assertLessThan(0.5, $elapsed, sprintf('%s parked behind %s read learnt of the drop after %.3f s, with a 3 s timeout, where that read ends after 2 s', $operation, $holder === 'poll' ? "a poll's" : "a flush's", $elapsed));
            self::assertSame($readsBefore + 1, $watched->reads, 'the request never read the socket');
            self::assertSame([], self::muxWakes($connection), 'no wake-up is left behind');

            // The other operation's read ends with the answer it waits for, as it would have.
            if ($holder === 'poll') {
                $transport->pushFrame(ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-1'));
                $message = $holding->await(new TimeoutCancellation(2));
                self::assertInstanceOf(NatsMessage::class, $message);
                self::assertSame('job-1', $message->payload);
            } else {
                $transport->pushFrame("PONG\r\n");
                $holding->await(new TimeoutCancellation(2));
            }
        } finally {
            $stop->cancel();
            $loop->await();
        }
    }

    /** @return iterable<string, array{string}> */
    public static function rejections(): iterable
    {
        yield 'the subscription limit' => ['limit'];
        yield 'permissions' => ['permissions'];
    }

    /**
     * The application closes the connection on the -ERR itself, read in the main case's shape: the permissions
     * violation is reported to its error listener, which calls disconnect(); 'maximum subscriptions exceeded' fails
     * the read that met it, and the application's loop calls disconnect() on that. The request the drop woke ends at
     * once with a ConnectionException, within 0.5 s of its 3 s timeout, and the connection ends Closed. The close
     * follows the rejection within the request's 1 ms pause, so it resets the rejection latch before the request
     * looks, and a woken read returns without reading: without the request's exit on the close, it looked every
     * millisecond until its deadline and ended with a timeout, where a request whose read was parked on the socket
     * used to fail with the closed connection.
     */
    #[DataProvider('rejections')]
    public function testARequestWokenByTheRejectionEndsWithTheClosedConnectionWhenTheApplicationClosesOnTheError(string $rejection): void
    {
        /** @var \ArrayObject<int, \Closure(): void> $closers What the error listener does on the permissions -ERR, once the client exists. */
        $closers = new \ArrayObject();
        $listener = static function (\Throwable $error) use ($closers): void {
            if (str_contains($error->getMessage(), 'Permissions Violation')) {
                foreach ($closers as $closer) {
                    $closer();
                }
            }
        };
        [$transport, $watched, $client] = $this->connectForTheDrop($listener);
        $close = static function () use ($client): void {
            $client->disconnect()->ignore();
        };
        $closers[] = $close;
        $closeOnTheLimit = static function (\Throwable $error) use ($close): void {
            if (str_contains($error->getMessage(), 'maximum subscriptions exceeded')) {
                $close();
            }
        };
        [$stop, $loop] = $this->startReadLoop($client, $rejection === 'limit' ? $closeOnTheLimit : null);
        $this->awaitRead($watched);

        try {
            $start = hrtime(true);
            $result = self::startRequest('request', $client, 3_000);
            $replyTo = $this->awaitPublished($transport);
            $this->dropTheInboxWhileTheOperationReads($transport, $watched, self::errFrame($rejection, self::inboxOf($replyTo)));
            [, $error] = self::settle($result);
            $elapsed = $this->secondsSince($start);

            self::assertInstanceOf(ConnectionException::class, $error, sprintf('the request ended after %.3f s with %s', $elapsed, self::describe($error)));
            self::assertLessThan(0.5, $elapsed, sprintf('the request ended after %.3f s, with a 3 s timeout, with %s', $elapsed, self::describe($error)));
            $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Closed);
            self::assertSame([], self::muxWakes(self::connectionOf($client)), 'no wake-up is left behind');
        } finally {
            $stop->cancel();
            $loop->await();
        }
    }

    /**
     * Guard: five requests wait at once, request() and requestMany() among them. One of their reads is on the socket
     * and the others wait for it, in their reads or at their own waits for the read slot, when the application's read
     * meets the -ERR in the main case's shape: every one of them ends at once with the dropped-inbox error or the
     * permissions error, within 0.5 s of their 3 s timeouts, no wake-up is left behind, and the application's read is
     * back on the socket, alone.
     */
    #[DataProvider('rejections')]
    public function testEveryOneOfManyWaitingRequestsEndsAtOnceWithTheDrop(string $rejection): void
    {
        [$transport, $watched, $client] = $this->connectForTheDrop();
        [$stop, $loop] = $this->startReadLoop($client);
        $this->awaitRead($watched);

        try {
            $results = [];
            for ($i = 0; $i < 5; $i++) {
                $results[] = self::startRequest($i % 2 === 1 ? 'requestMany' : 'request', $client, 3_000);
            }
            $this->waitUntil(static fn(): bool => count($transport->controlLinesStartingWith('PUB svc.held ')) === 5);
            $replyTo = $this->awaitPublished($transport);

            $start = hrtime(true);
            $this->dropTheInboxWhileTheOperationReads($transport, $watched, self::errFrame($rejection, self::inboxOf($replyTo)));
            $errors = [];
            foreach ($results as $result) {
                [, $errors[]] = self::settle($result);
            }
            $elapsed = $this->secondsSince($start);

            foreach ($errors as $i => $error) {
                self::assertInstanceOf(ConnectionException::class, $error, sprintf('request %d ended with %s', $i, self::describe($error)));
                self::assertStringContainsString($rejection === 'limit' ? self::MUX_DROPPED : self::MUX_REJECTED, $error->getMessage());
            }
            self::assertLessThan(0.5, $elapsed, sprintf('the five requests all ended after %.3f s', $elapsed));
            self::assertSame([], self::muxWakes(self::connectionOf($client)), 'no wake-up is left behind');
            $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        } finally {
            $stop->cancel();
            $loop->await();
        }
    }

    /** @return iterable<string, array{string, string, string}> */
    public static function replyPositionsAndRejections(): iterable
    {
        foreach (['request()' => 'request', 'requestMany(max 1)' => 'requestMany'] as $name => $operation) {
            foreach (['the subscription limit' => 'limit', 'permissions' => 'permissions'] as $rejectionName => $rejection) {
                yield $name . ', the reply ahead of the -ERR for ' . $rejectionName => [$operation, 'ahead', $rejection];
                yield $name . ', the reply behind the -ERR for ' . $rejectionName => [$operation, 'behind', $rejection];
            }
        }
    }

    /**
     * The request's reply is in the same chunk as the -ERR, read by the application's loop in the main case's shape,
     * and the chunk's order and the -ERR decide, as they did. Ahead of a 'maximum subscriptions exceeded', the reply
     * confirms the inbox as its frame is handled, the -ERR leaves the inbox alone and the request gets the reply, on an
     * inbox the next request is answered on (a guard, which passed before the fix too: the reply wakes the request).
     * Behind it, the reply is addressed to a subscription the client dropped with the -ERR, so it is discarded, and the
     * request fails at once with the dropped-inbox error. A permissions violation naming the inbox drops the
     * subscription with whatever is queued on it, so the reply is discarded ahead of the -ERR as well as behind it, and
     * the request fails at once with the permissions error, a requestMany() with nothing collected too (#167). Each
     * within 0.5 s of the 3 s timeout, where the request used to wait it out.
     */
    #[DataProvider('replyPositionsAndRejections')]
    public function testARequestWhoseReplyIsInTheSameChunkAsTheErrGetsWhatTheChunksOrderDecides(string $operation, string $position, string $rejection): void
    {
        [$transport, $watched, $client] = $this->connectForTheDrop();
        [$stop, $loop] = $this->startReadLoop($client);
        $this->awaitRead($watched);

        try {
            $start = hrtime(true);
            $result = self::startRequest($operation, $client, 3_000);
            $replyTo = $this->awaitPublished($transport);
            $reply = ReconnectingTransport::msgFrame($replyTo, (int) $transport->sidFor($replyTo), 'late');
            $err = self::errFrame($rejection, self::inboxOf($replyTo));
            $this->dropTheInboxWhileTheOperationReads($transport, $watched, $position === 'ahead' ? $reply . $err : $err . $reply);
            [$payloads, $error] = self::settle($result);
            $elapsed = $this->secondsSince($start);

            self::assertLessThan(0.5, $elapsed, sprintf('%s ended after %.3f s, with a 3 s timeout, with %s', $operation, $elapsed, self::describe($error)));
            if ($rejection === 'limit' && $position === 'ahead') {
                self::assertNull($error, 'the request got its reply: ' . self::describe($error));
                self::assertSame(['late'], $payloads);
                self::assertSame('ok', $client->request('svc.echo', 'y', 2_000)->await()->payload);
                self::assertCount(1, $transport->controlLinesStartingWith('SUB _INBOX.'), 'the reply confirmed the inbox, which the -ERR behind it left alone');
            } elseif ($rejection === 'limit') {
                self::assertInstanceOf(ConnectionException::class, $error, 'the request failed: ' . self::describe($error));
                self::assertStringContainsString(self::MUX_DROPPED, $error->getMessage());
                $transport->answerPings = true;
                self::assertSame('ok', $client->request('svc.echo', 'y', 2_000)->await()->payload);
                self::assertCount(2, $transport->controlLinesStartingWith('SUB _INBOX.'), 'the next request subscribed a new inbox');
            } else {
                self::assertInstanceOf(ConnectionException::class, $error, 'the request failed, its reply discarded with the subscription: ' . self::describe($error));
                self::assertStringContainsString(self::MUX_REJECTED, $error->getMessage());
                try {
                    $client->request('svc.echo', 'y', 3_000)->await();
                    self::fail('expected the next request to fail: request/reply is unavailable until the connection closes for good');
                } catch (ConnectionException $e) {
                    self::assertStringContainsString(self::MUX_REJECTED, $e->getMessage());
                }
                self::assertCount(1, $transport->controlLinesStartingWith('SUB _INBOX.'), 'the latch kept the next request from subscribing again');
            }
        } finally {
            $stop->cancel();
            $loop->await();
        }
    }

    /**
     * Guard, no change expected: after a drop, a request waits for its new inbox to be confirmed, reading for the PONG
     * of the PING behind the new SUB. The server rejects that SUB too, behind a PING of its own, and the application's
     * read brings the chunk in the main case's shape, the request's read parked on the socket when the -ERR drops the
     * inbox: the PONG that follows the -ERR completes the pong slot the request reads for, which wakes its read, and
     * the request fails within 0.5 s of its 3 s timeout, without being sent. That request has no waiter yet, so #180's
     * wake-up plays no part.
     */
    public function testARequestWaitingForTheNewInboxToBeConfirmedStillFailsAtOnceWhenTheServerRejectsItAgain(): void
    {
        [$transport, $watched, $client] = $this->connectForTheDrop();
        [$stop, $loop] = $this->startReadLoop($client);
        $this->awaitRead($watched);

        try {
            // The first request's inbox is dropped (the main case), so the next request waits for its new one.
            $first = self::startRequest('request', $client, 3_000);
            $this->awaitPublished($transport);
            $this->dropTheInboxWhileTheOperationReads($transport, $watched, self::errFrame('limit', ''));
            [, $error] = self::settle($first);
            self::assertInstanceOf(ConnectionException::class, $error, 'the first request failed: ' . self::describe($error));
            self::assertStringContainsString(self::MUX_DROPPED, $error->getMessage());
            // The application's read is back on the socket, alone.
            $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);

            // The server answers the new SUB and its PING as before, in a chunk behind a PING of its own; the PONG's
            // write is held up until the request's read, for the confirmation, has taken the socket.
            $transport->stallNextWriteContaining('PONG', 5.0);
            $transport->afterWrite = static function (string $bytes) use ($transport): void {
                if (str_contains($bytes, 'SUB _INBOX.')) {
                    $transport->pushFrame("PING\r\n-ERR 'maximum subscriptions exceeded'\r\nPONG\r\n");
                }
            };
            $reading = self::nextRead($watched);
            $start = hrtime(true);
            $second = self::startRequest('request', $client, 3_000);
            $this->waitUntil(static fn(): bool => $reading->isComplete() && $transport->writesStalled() > 0);
            $watched->onRead = null;
            $transport->releaseStalledWrites();
            [, $error] = self::settle($second);
            $elapsed = $this->secondsSince($start);

            self::assertInstanceOf(ConnectionException::class, $error, sprintf('the second request ended after %.3f s with %s', $elapsed, self::describe($error)));
            self::assertStringContainsString(self::MUX_DROPPED, $error->getMessage());
            self::assertLessThan(0.5, $elapsed, sprintf('the second request failed after %.3f s, with a 3 s timeout', $elapsed));
            self::assertCount(1, $transport->controlLinesStartingWith('PUB svc.held'), 'the second request was not sent');
            self::assertCount(2, $transport->controlLinesStartingWith('SUB _INBOX.'));
        } finally {
            $transport->afterWrite = null;
            $stop->cancel();
            $loop->await();
        }
    }

    /**
     * Guard: the wake-up is registered with the waiter and removed with it, so none is left behind once a request ends
     * with its reply, its replies, its timeout or its cancellation. The drop is the main case, which checks the same.
     */
    public function testNoWakeUpIsLeftBehindOnceARequestEnds(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $transport->responder = static fn(string $subject, ?string $replyTo): array => $replyTo === null || $subject === 'svc.held'
            ? []
            : $transport->replyFrame($replyTo, 'ok');

        self::assertSame('ok', $connection->request('svc.echo', 'x', 1_000)->await()->payload);
        self::assertSame([], self::muxWakes($connection), 'after a reply');

        self::assertSame(['ok'], self::payloads($connection->requestMany('svc.echo', 'x', null, 1, 1_000)->await()));
        self::assertSame([], self::muxWakes($connection), 'after requestMany() collected its replies');

        try {
            $connection->request('svc.held', 'x', 30)->await();
            self::fail('expected the request to time out');
        } catch (TimeoutException) {
            // Nothing answers svc.held.
        }
        self::assertSame([], self::muxWakes($connection), 'after a timeout');

        $cancel = new DeferredCancellation();
        EventLoop::delay(0.02, static fn() => $cancel->cancel());
        try {
            $connection->request('svc.held', 'x', 3_000, $cancel->getCancellation())->await();
            self::fail('expected the request to be cancelled');
        } catch (CancelledException) {
            // The caller gave up.
        }
        self::assertSame([], self::muxWakes($connection), 'after a cancellation');
    }

    /**
     * Guard, white-box: a drop fires the wake-up of every request waiting on the inbox, once, and nothing else - not the
     * wake-up of a request that ended, removed with its waiter, nor the one a requestMany() waiter rotated away from,
     * and with no request waiting there is nothing to fire. One registered after a drop fires with the next.
     */
    public function testADropFiresTheWakeUpOfEveryRequestWaitingAndNothingElse(): void
    {
        $connection = $this->own(new NatsConnection(new NatsOptions(), new FakeTransport()));
        $noop = static function (NatsMessage $message): void {};
        $wake = new \ReflectionMethod(NatsConnection::class, 'wakeMuxWaiters');
        $register = new \ReflectionMethod(NatsConnection::class, 'registerMuxWaiter');
        $remove = new \ReflectionMethod(NatsConnection::class, 'removeMuxWaiter');
        $renew = new \ReflectionMethod(NatsConnection::class, 'renewMuxWake');

        $wake->invoke($connection);
        self::assertSame([], self::muxWakes($connection), 'with no request waiting there is nothing to fire');

        $waiting = new DeferredCancellation();
        $gone = new DeferredCancellation();
        $rotatedAwayFrom = new DeferredCancellation();
        $current = new DeferredCancellation();
        $register->invoke($connection, 'a', $noop, $waiting);
        $register->invoke($connection, 'b', $noop, $gone);
        $remove->invoke($connection, 'b');
        $register->invoke($connection, 'c', $noop, $rotatedAwayFrom);
        $renew->invoke($connection, 'c', $current);

        $wake->invoke($connection);

        self::assertTrue($waiting->isCancelled(), 'the wake-up of a request waiting on the inbox fires');
        self::assertFalse($gone->isCancelled(), 'not the one of a request that ended');
        self::assertTrue($current->isCancelled(), 'the wake-up a waiter rotated to fires');
        self::assertFalse($rotatedAwayFrom->isCancelled(), 'not the one it rotated away from');
        self::assertCount(2, self::muxWakes($connection), 'the wake-ups stay registered until their requests remove them');

        $later = new DeferredCancellation();
        $register->invoke($connection, 'd', $noop, $later);
        self::assertFalse($later->isCancelled(), 'a wake-up registered after a drop waits for the next one');
        $wake->invoke($connection);
        self::assertTrue($later->isCancelled());
    }

    /**
     * Guard: a drop wakes the requests waiting on the inbox and nothing else. A poll's read is parked on the socket when
     * another fiber's read drops the inbox, with no request waiting on it: the inbox is dropped, the poll's read stays the
     * only read on the socket, and the poll gets its own message on it. A poll's or a fetch's read wakes on a delivery to
     * its own subscription alone.
     */
    public function testAPollsReadIsNotEndedByADropOfTheInbox(): void
    {
        [$transport, $watched, $client] = $this->connectForTheDrop();
        // An inbox the server has not confirmed, with no request waiting on it: a request that gives up leaves it in place.
        try {
            $client->request('svc.held', 'x', 30)->await();
            self::fail('expected the request to time out');
        } catch (TimeoutException) {
            // Nothing answers svc.held.
        }
        $connection = self::connectionOf($client);
        $generation = self::muxGeneration($connection);
        $queue = $client->subscribeQueue('jobs')->await();
        [$stop, $loop] = $this->startReadLoop($client);
        $this->awaitRead($watched);

        try {
            $poll = async(static fn(): ?NatsMessage => $queue->setTimeout(3.0)->next());
            // The poll's read goes to wait behind the application's read.
            delay(0.02);
            self::assertFalse($poll->isComplete());

            $this->dropTheInboxWhileTheOperationReads($transport, $watched, self::errFrame('limit', ''));
            $this->waitUntil(static fn(): bool => self::muxGeneration($connection) !== $generation);
            $readsAfterTheDrop = $watched->reads;
            // Room for a wake-up, had the drop fired one for the poll, to end the poll's read and the poll to read again.
            delay(0.02);

            self::assertFalse($poll->isComplete(), 'the poll waits on');
            self::assertSame($readsAfterTheDrop, $watched->reads, "the poll's read is still the one on the socket");
            self::assertSame(1, $watched->readsUnderWay);

            $transport->pushFrame(ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-1'));
            self::assertSame('job-1', $poll->await(new TimeoutCancellation(2))?->payload);
        } finally {
            $stop->cancel();
            $loop->await();
        }
    }

    /**
     * Guard: a drop with no request waiting fires nothing and costs nothing. The inbox a request that timed out left
     * unconfirmed is dropped by an -ERR the application's read brings: no wake-up is registered, and the only read
     * that follows is the one the application's loop makes after any failed read.
     */
    public function testADropWithNoRequestWaitingCostsNoRead(): void
    {
        [$transport, $watched, $client] = $this->connectForTheDrop();
        try {
            $client->request('svc.held', 'x', 30)->await();
            self::fail('expected the request to time out');
        } catch (TimeoutException) {
            // Nothing answers svc.held: an inbox the server has not confirmed is left, with no request waiting on it.
        }
        $connection = self::connectionOf($client);
        $generation = self::muxGeneration($connection);
        [$stop, $loop] = $this->startReadLoop($client);
        $this->awaitRead($watched);

        try {
            $reads = $watched->reads;
            $transport->pushFrame("PING\r\n-ERR 'maximum subscriptions exceeded'\r\nPONG\r\n");
            $this->waitUntil(static fn(): bool => self::muxGeneration($connection) !== $generation);
            // Room for anything the drop might have set off.
            delay(0.05);

            self::assertSame([], self::muxWakes($connection), 'nothing was registered, so nothing fired');
            self::assertSame($reads + 1, $watched->reads, "only the application's loop read again, after the read the -ERR failed");
            self::assertSame(1, $watched->readsUnderWay);
        } finally {
            $stop->cancel();
            $loop->await();
        }
    }

    /**
     * Guard: a request in flight across a reconnect whose replay of the inbox is rejected for the limit. The request's
     * read waits for the recovery, whose own read meets the -ERR: the replayed inbox is dropped and the request's
     * wake-up fired during the recovery. The request fails at once with the dropped-inbox error and leaves no wake-up
     * behind, and the recovery completes, the connection Open. A retry issued at once waits for the recovery, then
     * subscribes a new inbox, which the server rejects while the limit holds, and fails without being sent; once a
     * slot is free the next request is answered. Over the server model in `tests/Support/SubscriptionLimitServer.php`.
     */
    public function testARequestInFlightAcrossAReconnectWhoseReplayedInboxIsRejectedFailsWithTheDrop(): void
    {
        $server = new SubscriptionLimitServer();
        $server->limit = 2;
        $server->responder = static fn(string $subject, ?string $replyTo, string $payload): array => $replyTo === null || $subject === 'svc.held'
            ? []
            : $server->replyFrame($replyTo, 'echo:' . $payload);
        $recorder = new LifecycleRecorder();
        $connection = $this->own(new NatsConnection(new NatsOptions(
            connectTimeoutMs: 500,
            reconnectEnabled: true,
            maxReconnectAttempts: 3,
            reconnectDelayMs: 1,
            reconnectMaxDelayMs: 1,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
            connectionListener: $recorder->connectionListener(),
            errorListener: $recorder->errorListener(),
            waitForReconnect: true,
        ), $server));
        $this->opened[] = $connection;
        $connection->connect()->await();
        // Replayed ahead of the inbox, so it keeps the one slot left after the reconnect.
        $appSid = $connection->subscribe('app.one', static function (): void {})->await();

        $request = $connection->request('svc.held', 'x', 3_000);
        $request->ignore();
        $this->waitUntil(static fn(): bool => $server->controlLines('PUB svc.held') !== []);
        self::assertCount(1, self::muxWakes($connection), 'the request waits with its wake-up registered');

        $server->limit = 1;
        $server->dropConnection();
        $start = hrtime(true);
        try {
            $request->await(new TimeoutCancellation(4));
            self::fail('expected the request to fail: the server rejected the replay of its inbox');
        } catch (ConnectionException $e) {
            self::assertStringContainsString(self::MUX_DROPPED, $e->getMessage());
        }
        self::assertLessThan(0.5, $this->secondsSince($start), 'the request failed with the drop, not at its timeout');
        self::assertSame([], self::muxWakes($connection), 'no wake-up is left behind');

        // What a caller does: retry at once.
        $retry = $connection->request('svc.echo', 'again', 2_000);
        $retry->ignore();
        $this->waitUntilOpen($connection);
        self::assertSame(1, $server->epoch());
        self::assertNotSame([], $recorder->errorsContaining('maximum subscriptions exceeded'), 'the replayed inbox SUB was rejected');
        try {
            $retry->await(new TimeoutCancellation(4));
            self::fail('expected the retry to fail while the limit holds');
        } catch (ConnectionException $e) {
            self::assertStringContainsString(self::MUX_DROPPED, $e->getMessage());
        }
        self::assertSame([], $server->controlLines('PUB svc.echo'), 'the retry was not sent');
        self::assertSame(['app.one'], array_values($server->heldSubscriptions()));

        $connection->unsubscribe($appSid)->await();
        self::assertSame('echo:two', $connection->request('svc.echo', 'two', 1_000)->await()->payload);
        self::assertSame([], self::muxWakes($connection));
    }

    /**
     * A connection over the scripted server, seen through a WatchedTransport, on which the reply inbox stays unconfirmed:
     * from the handshake on the server does not answer PINGs, so the PONG that would confirm the inbox the first request
     * subscribes never comes, and the server may still reject its SUB as far as the client knows. "svc.held" is never
     * answered, so a request to it waits for its reply; anything else is answered with 'ok'.
     *
     * @param (\Closure(\Throwable): void)|null $errorListener The application's error listener, which a permissions
     *        violation is reported to.
     * @return array{ReconnectingTransport, WatchedTransport, NatsClient}
     */
    private function connectForTheDrop(?\Closure $errorListener = null): array
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = $this->own(new NatsClient($this->options(true, 2_000, 1_000, 0, 2, null, 5, 20, $errorListener), $watched));
        $this->opened[] = $client;
        $client->connect()->await();
        $transport->answerPings = false;
        $transport->responder = static fn(string $subject, ?string $replyTo): array => $replyTo === null || $subject === 'svc.held'
            ? []
            : $transport->replyFrame($replyTo, 'ok');

        return [$transport, $watched, $client];
    }

    /**
     * An application loop that keeps a read on the socket, as a consumer's does, and reads on past whatever a read
     * throws: an -ERR the server keeps the connection open for fails the read that brings it. $onFailure, if given, is
     * called with what a read threw before the loop reads on. Stop it with the cancellation, then await the future.
     *
     * @param (\Closure(\Throwable): void)|null $onFailure
     * @return array{DeferredCancellation, Future<void>}
     */
    private function startReadLoop(NatsClient $client, ?\Closure $onFailure = null): array
    {
        $stop = new DeferredCancellation();
        $loop = async(static function () use ($client, $stop, $onFailure): void {
            while (!$stop->isCancelled()) {
                try {
                    $client->processIncoming($stop->getCancellation())->await();
                } catch (CancelledException) {
                    return;
                } catch (\Throwable $e) {
                    if ($onFailure !== null) {
                        $onFailure($e);
                    }
                    delay(0.001);
                }
            }
        });

        return [$stop, $loop];
    }

    /** Waits until a read takes the socket of $watched. */
    private function awaitRead(WatchedTransport $watched): void
    {
        self::nextRead($watched)->getFuture()->await(new TimeoutCancellation(2));
        $watched->onRead = null;
    }

    /**
     * Completed when the next read takes the socket of $watched.
     *
     * @return DeferredFuture<null>
     */
    private static function nextRead(WatchedTransport $watched): DeferredFuture
    {
        /** @var DeferredFuture<null> $reading */
        $reading = new DeferredFuture();
        $watched->onRead = static function () use ($reading): void {
            if (!$reading->isComplete()) {
                $reading->complete();
            }
        };

        return $reading;
    }

    /**
     * A request to "svc.held", which nothing answers, as request() or requestMany(max 1); its payloads.
     *
     * @return Future<list<string>>
     */
    private static function startRequest(string $operation, NatsClient $client, int $timeoutMs): Future
    {
        $result = $operation === 'request'
            ? $client->request('svc.held', 'x', $timeoutMs)->map(static fn(NatsMessage $message): array => [$message->payload])
            : $client->requestMany('svc.held', 'x', null, 1, $timeoutMs)->map(self::payloads(...));
        $result->ignore();

        return $result;
    }

    /** Waits until the client has published its request to "svc.held", and returns the reply subject it was sent with. */
    private function awaitPublished(ReconnectingTransport $transport): string
    {
        $this->waitUntil(static fn(): bool => $transport->controlLinesStartingWith('PUB svc.held ') !== []);
        $line = $transport->controlLinesStartingWith('PUB svc.held ')[0];
        if (preg_match('/^PUB svc\.held (\S+) \d+$/', $line, $parts) !== 1) {
            self::fail('not a request with a reply subject: ' . $line);
        }

        return $parts[1];
    }

    /** The reply inbox's wildcard subject, "<base>.*", from a reply subject "<base>.<token>" on it. */
    private static function inboxOf(string $replyTo): string
    {
        return substr($replyTo, 0, (int) strrpos($replyTo, '.')) . '.*';
    }

    /**
     * The #180 shape. The application's read is on the socket, the operation's read parked behind it. The server's next
     * chunk holds a PING and, behind it, $frames, ending with the -ERR that drops or rejects the inbox, then the PONG
     * answering the PING the client wrote behind the inbox's SUB, which the server answers after the SUB, in order. The
     * application's read takes the chunk and its dispatch answers the PING, with the PONG's write held up as on a socket
     * under backpressure; the operation's read takes the socket meanwhile, and only then is the write released, so the
     * dispatch meets the -ERR with the operation's read parked on the socket. Decided by the order of events, not timed.
     */
    private function dropTheInboxWhileTheOperationReads(ReconnectingTransport $transport, WatchedTransport $watched, string $frames): void
    {
        $transport->stallNextWriteContaining('PONG', 5.0);
        $reading = self::nextRead($watched);
        $transport->pushFrame("PING\r\n" . $frames . "PONG\r\n");
        $this->waitUntil(static fn(): bool => $reading->isComplete() && $transport->writesStalled() > 0);
        $watched->onRead = null;
        $transport->releaseStalledWrites();
    }

    /** The -ERR rejecting a SUB for the subscription limit, which names nothing, or for permissions, which names $inbox. */
    private static function errFrame(string $rejection, string $inbox): string
    {
        return $rejection === 'limit'
            ? "-ERR 'maximum subscriptions exceeded'\r\n"
            : "-ERR 'Permissions Violation for Subscription to \"" . $inbox . "\"'\r\n";
    }

    /**
     * Awaits $result, within 4 s: its payloads and what it threw.
     *
     * @param Future<list<string>> $result
     * @return array{list<string>, ?\Throwable}
     */
    private static function settle(Future $result): array
    {
        try {
            return [$result->await(new TimeoutCancellation(4)), null];
        } catch (\Throwable $e) {
            return [[], $e];
        }
    }

    private static function describe(?\Throwable $error): string
    {
        return $error === null ? 'a result' : $error::class . ': ' . $error->getMessage();
    }

    private static function connectionOf(NatsClient $client): NatsConnection
    {
        $connection = (new \ReflectionProperty(NatsClient::class, 'connection'))->getValue($client);
        self::assertInstanceOf(NatsConnection::class, $connection);

        return $connection;
    }

    /** @return array<array-key, mixed> The wake-ups registered with the request waiters, by token. */
    private static function muxWakes(NatsConnection $connection): array
    {
        $wakes = (new \ReflectionProperty(NatsConnection::class, 'muxWakes'))->getValue($connection);
        self::assertIsArray($wakes);

        return $wakes;
    }

    private static function muxGeneration(NatsConnection $connection): int
    {
        $generation = (new \ReflectionProperty(NatsConnection::class, 'muxGeneration'))->getValue($connection);
        self::assertIsInt($generation);

        return $generation;
    }

    /**
     * @param list<NatsMessage> $messages
     * @return list<string>
     */
    private static function payloads(array $messages): array
    {
        return array_map(static fn(NatsMessage $message): string => $message->payload, $messages);
    }
}
