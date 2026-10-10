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
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\JetStreamContext;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\SubscriptionLimitServer;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Amp\async;
use function Amp\delay;

/**
 * The JetStream reply inboxes at the connection's subscription limit (#175): the pull fetch's ("_INBOX.JS.FETCH.<nuid>",
 * fetchNext() goes through fetchBatch()), the pull pipeline's ("_INBOX.JS.PULL.<nuid>.*") and the batched Direct Get's
 * ("_INBOX.JS.DGET.<nuid>"). The server answers a SUB beyond the limit with "-ERR 'maximum subscriptions exceeded'",
 * which names no subject, and keeps the connection open. An operation whose own read met the -ERR failed with it; when
 * another fiber's read took it - the heartbeat's while the SUB write was held up, an application's processIncoming()
 * loop's - the operation learnt nothing and waited out its deadline: the fetch and the Direct Get batch reported a
 * routine empty result (a 408, a stall) after the expiry plus a second, the pull pipeline retired pull after pull at
 * their deadlines. The inboxes now get the shared reply inbox's rule (NatsConnection::subscribeGuarded()): a PING
 * behind the SUB, whose PONG confirms the inbox, as a delivery on it does; a limit -ERR read before that, by whichever
 * fiber, drops the inbox, writes its UNSUB and fails the operation at once with an error naming the inbox and the
 * limit; several inboxes unconfirmed at once, the shared reply inbox among them, are all treated as rejected. A
 * reconnect replays an inbox under the same rule, also one whose own SUB write failed, when the subscribe itself
 * reports the rejection.
 *
 * Over `tests/Support/SubscriptionLimitServer.php`, which enforces the limit and answers in wire order, with an
 * application loop as the other reader, and over `tests/Support/ReconnectingTransport.php` with the heartbeat as the
 * other reader and the SUB write held up. The order of events decides; the time bounds (1 s) sit far from both the
 * expected behaviour (milliseconds) and the broken one (the deadline: 3 s with the 2 s expiry used here). Each
 * regression test fails on the code before the fix with the operation's routine result after its deadline; the guards
 * pass on both unless said otherwise.
 */
final class JetStreamInboxRejectionTest extends TestCase
{
    private const LIMIT = 'because the connection is at its subscription limit (maximum subscriptions exceeded)';
    private const SERVER_ERR = "Server sent error frame: 'maximum subscriptions exceeded'";
    private const MUX_DROPPED = 'the server may have rejected the shared reply-inbox subscription';
    private const PULL_SUBJECT = '$JS.API.CONSUMER.MSG.NEXT.S.C';
    private const DIRECT_GET_SUBJECT = '$JS.API.DIRECT.GET.S';

    /** @var list<NatsClient> */
    private array $opened = [];

    /** @var \ArrayObject<int, string> The reply subjects of the pulls the scripted server received, in order. */
    private \ArrayObject $pulls;

    /** Whether the scripted server answers a pull at once, with one message, or holds it. */
    private bool $answerPulls = true;

    protected function setUp(): void
    {
        $this->pulls = new \ArrayObject();
        $this->answerPulls = true;
    }

    protected function tearDown(): void
    {
        foreach ($this->opened as $client) {
            try {
                $client->disconnect()->await(new TimeoutCancellation(1));
            } catch (\Throwable) {
                // Already closed.
            }
        }

        $this->opened = [];
        delay(0.02);
    }

    /** @return iterable<string, array{string}> */
    public static function fetches(): iterable
    {
        yield 'fetchBatch()' => ['fetchBatch'];
        yield 'fetchNext()' => ['fetchNext'];
    }

    /**
     * The issue's shape. The heartbeat reads the socket every 50 ms. The fetch's SUB write is held up, like a write
     * under backpressure, so the fetch is suspended in its subscribe when the server's -ERR arrives, and the heartbeat's
     * read brings it: the heartbeat reports it to the error listener, the only reader there is. Once the write goes on
     * the fetch fails at once, within 1 s, with the error naming its inbox and the limit, where it used to report an
     * empty pull (JetStreamException 408, 'No messages received within timeout') after the 2 s expiry plus 1 s of slack;
     * no pull is sent for the dead inbox, the inbox is unsubscribed, the connection is Open, nothing is recorded for the
     * dead sid by the marks the fetch applies once its write completes, and a retry once the limit has room subscribes
     * a new inbox, which the server confirms, and gets its message.
     *
     * The server leaves PINGs unanswered while the SUB is held up, and the heartbeat ticks at least once meanwhile, as
     * it does on a loaded machine. A socket keeps the order of its writes, and the production transports hand their
     * bytes over synchronously, but this double lets a write made during a stall overtake the stalled one: a heartbeat
     * PING written past the held-up SUB+PING would be answered first, and its PONG, completing the oldest pong slot,
     * would confirm the inbox before its SUB reached the server. With no PONG the inbox stays unconfirmed, whenever the
     * heartbeat ticks, and the heartbeat's reads still bring the -ERR.
     */
    #[DataProvider('fetches')]
    public function testAFetchWhoseInboxSubIsRejectedWhileTheHeartbeatReadsTheErrFailsAtOnce(string $fetch): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = new NatsClient(new NatsOptions(
            connectTimeoutMs: 500,
            reconnectEnabled: false,
            pingIntervalSeconds: 0.05,
            // The heartbeat's PINGs go unanswered until the retry: far more ticks than the test takes.
            maxPingsOut: 100,
            errorListener: $recorder->errorListener(),
        ), $transport);
        $this->opened[] = $client;
        $client->connect()->await();
        $transport->answerPings = false;
        $this->answerPulls = false;
        $transport->responder = function (string $subject, ?string $replyTo) use ($transport): array {
            if (!$this->answerPulls || $replyTo === null || $subject !== self::PULL_SUBJECT) {
                return [];
            }

            return [ReconnectingTransport::msgFrame('evt.s', (int) $transport->sidFor($replyTo), 'm1', '$JS.ACK.S.C.1.1.1.0.0')];
        };
        $js = $client->jetStream();

        $transport->stallNextWriteContaining('SUB _INBOX.JS.FETCH', 5.0);
        $fetching = self::startOperation($fetch, $js);
        $this->waitUntil(static fn(): bool => $transport->writesStalled() > 0);
        // The heartbeat ticks while the write is held up: its PING overtakes the SUB in this double, unanswered.
        delay(0.06);
        self::assertGreaterThan(1, count($transport->controlLinesStartingWith('PING')), 'the heartbeat wrote a PING past the held-up SUB');
        // The server rejects the SUB. With the write still held up, the heartbeat's reads are the only ones.
        $transport->pushFrame("-ERR 'maximum subscriptions exceeded'\r\n");
        $this->waitUntil(static fn(): bool => $recorder->errorsContaining('maximum subscriptions exceeded') !== []);
        self::assertFalse($fetching->isComplete(), 'the fetch is still in its subscribe, its SUB write held up');

        $start = hrtime(true);
        $transport->releaseStalledWrites();
        [, $error] = self::settle($fetching);
        $elapsed = self::secondsSince($start);

        self::assertInstanceOf(ConnectionException::class, $error, sprintf('%s ended after %.3f s with %s', $fetch, $elapsed, self::describe($error)));
        self::assertStringContainsString(self::LIMIT, $error->getMessage());
        self::assertStringContainsString('"_INBOX.JS.FETCH.', $error->getMessage(), 'the error names the inbox');
        self::assertLessThan(1.0, $elapsed, sprintf('%s failed after %.3f s, with a 2 s expiry and 1 s of slack', $fetch, $elapsed));
        self::assertSame([], $transport->controlLinesStartingWith('PUB ' . self::PULL_SUBJECT), 'no pull was sent for the dead inbox');
        $sid = self::lastSidOf($transport->controlLinesStartingWith('SUB _INBOX.JS.FETCH'));
        self::assertContains('UNSUB ' . $sid, $transport->controlLines(), 'the inbox was unsubscribed');
        self::assertSame(ConnectionState::Open, $client->state());
        // The slow-consumer exemption the fetch applies once its SUB write completes ran after the drop.
        self::assertGuardStateEmpty(self::connectionOf($client), 'after the rejected fetch');

        // The retry's inbox is confirmed by the PONG behind its SUB, as usual.
        $transport->answerPings = true;
        $this->answerPulls = true;
        [$payloads, $error] = self::settle(self::startOperation($fetch, $js));
        self::assertNull($error, 'the retry got its message: ' . self::describe($error));
        self::assertSame(['m1'], $payloads);
        self::assertCount(2, $transport->controlLinesStartingWith('SUB _INBOX.JS.FETCH'), 'the retry subscribed a new inbox');
    }

    /**
     * The same over the server model that enforces the limit, with an application loop as the other reader: one
     * subscription holds the one slot there is, the loop's read is on the socket, and the fetch's SUB is rejected, the
     * -ERR failing the loop's read. The fetch fails at once with the error naming its inbox and the limit, within 1 s
     * of its 3 s deadline, its inbox is unsubscribed and the connection Open; once the slot is free a retry subscribes a
     * new inbox, gets its message, and releases the inbox again.
     */
    public function testAFetchWhoseInboxSubIsRejectedWhileAnApplicationLoopReadsTheErrFailsAtOnce(): void
    {
        $server = $this->pullServer(limit: 1);
        $client = $this->connect($server);
        $fillSid = $this->subscribe($client, 'fill.a');
        [$stop, $loop, $failures] = $this->startReadLoop($client);

        try {
            $start = hrtime(true);
            [, $error] = self::settle(self::startOperation('fetchBatch', $client->jetStream()));
            $elapsed = self::secondsSince($start);

            self::assertInstanceOf(ConnectionException::class, $error, sprintf('the fetch ended after %.3f s with %s', $elapsed, self::describe($error)));
            self::assertStringContainsString(self::LIMIT, $error->getMessage());
            self::assertLessThan(1.0, $elapsed, sprintf('the fetch failed after %.3f s, with a 3 s deadline', $elapsed));
            self::assertNotSame([], self::messagesContaining($failures, 'maximum subscriptions exceeded'), "the loop's read met the -ERR");
            $sid = self::lastSidOf($server->controlLines('SUB _INBOX.JS.FETCH'));
            self::assertContains('UNSUB ' . $sid, $server->controlLines('UNSUB'), 'the inbox was unsubscribed');
            self::assertSame(ConnectionState::Open, $client->state());
            self::assertSame(['fill.a'], array_values($server->heldSubscriptions()));

            $client->unsubscribe($fillSid)->await();
            [$payloads, $error] = self::settle(self::startOperation('fetchBatch', $client->jetStream()));
            self::assertNull($error, 'the retry got its message: ' . self::describe($error));
            self::assertSame(['m1'], $payloads);
            self::assertSame([], $server->heldSubscriptions(), 'the retry released its inbox');
        } finally {
            $stop->cancel();
            $loop->await();
        }
    }

    /**
     * A batched Direct Get in the same shape: its inbox SUB is rejected, the loop's read brings the -ERR, and the call
     * fails at once with the error naming its inbox and the limit, within 1 s, where it used to report a stalled batch
     * ('no progress for 3000 ms') after its 2 s expiry plus 1 s of slack. Once the slot is free a retry completes, with
     * the empty batch the server's end-of-batch marker ends.
     */
    public function testADirectGetBatchWhoseInboxSubIsRejectedWhileAnotherReadBringsTheErrFailsAtOnce(): void
    {
        $server = $this->pullServer(limit: 1);
        $client = $this->connect($server);
        $fillSid = $this->subscribe($client, 'fill.a');
        [$stop, $loop] = $this->startReadLoop($client);

        try {
            $start = hrtime(true);
            [, $error] = self::settle(self::startOperation('directGetBatch', $client->jetStream()));
            $elapsed = self::secondsSince($start);

            self::assertInstanceOf(ConnectionException::class, $error, sprintf('the Direct Get batch ended after %.3f s with %s', $elapsed, self::describe($error)));
            self::assertStringContainsString(self::LIMIT, $error->getMessage());
            self::assertStringContainsString('Direct Get batch for stream "S" failed', $error->getMessage());
            self::assertStringContainsString('"_INBOX.JS.DGET.', $error->getMessage(), 'the error names the inbox');
            self::assertLessThan(1.0, $elapsed, sprintf('the Direct Get batch failed after %.3f s, with a 3 s stall interval', $elapsed));
            $sid = self::lastSidOf($server->controlLines('SUB _INBOX.JS.DGET'));
            self::assertContains('UNSUB ' . $sid, $server->controlLines('UNSUB'), 'the inbox was unsubscribed');
            self::assertSame(ConnectionState::Open, $client->state());

            $client->unsubscribe($fillSid)->await();
            [$payloads, $error] = self::settle(self::startOperation('directGetBatch', $client->jetStream()));
            self::assertNull($error, 'the retry completed: ' . self::describe($error));
            self::assertSame([], $payloads);
        } finally {
            $stop->cancel();
            $loop->await();
        }
    }

    /**
     * The pull pipeline in the same shape: its inbox SUB is rejected and the loop's read brings the -ERR. The run fails
     * fast with a JetStreamException saying the inbox may have been rejected for the subscription limit, within 1 s,
     * where it used to retire its pull at the pull's deadline, 3 s here, and return as a run that processed nothing
     * (the test waits at most 2 s for it). The inbox is unsubscribed and the connection Open; once the slot is free a
     * run gets its message.
     */
    public function testAPullConsumerWhoseInboxSubIsRejectedWhileAnotherReadBringsTheErrFailsFast(): void
    {
        $server = $this->pullServer(limit: 1);
        $client = $this->connect($server);
        $fillSid = $this->subscribe($client, 'fill.a');
        [$stop, $loop] = $this->startReadLoop($client);

        try {
            $js = $client->jetStream();
            $start = hrtime(true);
            $run = $js->pullConsumer('S', 'C')->setExpiresMs(2_000)->setIterations(1)->handle(static function (): void {});
            try {
                $run->await(new TimeoutCancellation(2.0));
                self::fail('expected the run to fail: the server rejected its inbox');
            } catch (JetStreamException $e) {
                self::assertStringContainsString('Pull consumer reply inbox "_INBOX.JS.PULL.', $e->getMessage());
                self::assertStringContainsString('may have been rejected by the server because the connection is at its subscription limit', $e->getMessage());
            }
            $elapsed = self::secondsSince($start);

            self::assertLessThan(1.0, $elapsed, sprintf('the run failed after %.3f s, with a 3 s pull deadline', $elapsed));
            $sid = self::lastSidOf($server->controlLines('SUB _INBOX.JS.PULL'));
            self::assertContains('UNSUB ' . $sid, $server->controlLines('UNSUB'), 'the inbox was unsubscribed');
            self::assertSame(ConnectionState::Open, $client->state());

            $client->unsubscribe($fillSid)->await();
            $received = [];
            $processed = $js->pullConsumer('S', 'C')->setExpiresMs(2_000)->setIterations(1)->handle(static function (NatsMessage $message) use (&$received): void {
                $received[] = $message->payload;
            })->await(new TimeoutCancellation(4));
            self::assertSame(1, $processed);
            self::assertSame(['m1'], $received);
        } finally {
            $stop->cancel();
            $loop->await();
        }
    }

    /**
     * Guard, no change expected: the fetch's own read meets the -ERR, nothing else reading, and the fetch fails with
     * the server's error at once, as it always did; the inbox is unsubscribed and the connection stays Open.
     */
    public function testTheOperationsOwnReadMeetingTheErrFailsItWithTheServersErrorAsBefore(): void
    {
        $server = $this->pullServer(limit: 1);
        $client = $this->connect($server);
        $this->subscribe($client, 'fill.a');

        $start = hrtime(true);
        [, $error] = self::settle(self::startOperation('fetchBatch', $client->jetStream()));
        $elapsed = self::secondsSince($start);

        self::assertInstanceOf(ConnectionException::class, $error, sprintf('the fetch ended after %.3f s with %s', $elapsed, self::describe($error)));
        self::assertStringContainsString(self::SERVER_ERR, $error->getMessage());
        self::assertLessThan(1.0, $elapsed);
        $sid = self::lastSidOf($server->controlLines('SUB _INBOX.JS.FETCH'));
        self::assertContains('UNSUB ' . $sid, $server->controlLines('UNSUB'), 'the inbox was unsubscribed');
        self::assertSame(ConnectionState::Open, $client->state());
    }

    /**
     * Guard: the PONG of the PING behind the inbox's SUB confirms the inbox. The server holds its answers back while a
     * fetch subscribes and sends its pull, which the server holds too, and while another subscription is rejected for
     * the limit; released, the PONG and the -ERR come in one chunk, read by the loop, which has held the socket all
     * along (the fetch's read waits behind it). The PONG confirms the inbox ahead of the -ERR, which then leaves it
     * alone and fails the loop's read: the fetch waits on, and gets its message once the server answers the pull, on
     * the one inbox it subscribed. Without the confirmation the -ERR would have failed the fetch. Only the order of
     * the chunk decides.
     */
    public function testThePongBehindTheSubConfirmsTheInboxWhichALaterLimitErrLeavesAlone(): void
    {
        $server = $this->pullServer(limit: 2);
        $client = $this->connect($server);
        $this->subscribe($client, 'fill.a');
        $this->answerPulls = false;
        [$stop, $loop, $failures] = $this->startReadLoop($client);

        try {
            $server->holdAnswers();
            $fetching = self::startOperation('fetchBatch', $client->jetStream());
            $this->waitUntil(fn(): bool => count($this->pulls) === 1);
            // Rejected: fill.a and the inbox hold both slots. Its -ERR is held behind the PONG of the inbox's PING.
            $this->subscribe($client, 'app.extra');
            $server->releaseAnswers();
            $this->waitUntil(static fn(): bool => self::messagesContaining($failures, 'maximum subscriptions exceeded') !== []);
            delay(0.02);
            self::assertFalse($fetching->isComplete(), 'the fetch waits on, its inbox confirmed');

            $server->pushFrame(implode('', self::dataFrameOn($server, $this->firstPull(), 'late')));
            [$payloads, $error] = self::settle($fetching);
            self::assertNull($error, 'the fetch got its message: ' . self::describe($error));
            self::assertSame(['late'], $payloads);
            self::assertCount(1, $server->controlLines('SUB _INBOX.JS.FETCH'));
        } finally {
            $stop->cancel();
            $loop->await();
        }
    }

    /**
     * Guard: a delivery on the inbox confirms it before the PONG behind its SUB, which the server here never sends. The
     * server holds its answers back while a fetch for two messages subscribes and sends its pull, answered with the
     * first message, and while another subscription is rejected for the limit; released, the message and the -ERR come
     * in one chunk, read by the loop, which has held the socket all along. The delivery confirms the inbox ahead of the
     * -ERR, which then leaves it alone: the fetch waits on, and returns both messages once the second arrives. Without
     * the confirmation the -ERR would have failed the fetch.
     */
    public function testADeliveryOnTheInboxConfirmsItBeforeThePongBehindItsSub(): void
    {
        $server = $this->pullServer(limit: 2);
        $client = $this->connect($server);
        $server->answerPings = false;
        $this->subscribe($client, 'fill.a');
        [$stop, $loop, $failures] = $this->startReadLoop($client);

        try {
            $server->holdAnswers();
            $fetching = $client->jetStream()->fetchBatch('S', 'C', 2, 2_000)->map(self::payloads(...));
            $fetching->ignore();
            $this->waitUntil(fn(): bool => count($this->pulls) === 1);
            // Rejected: fill.a and the inbox hold both slots. Its -ERR is held behind the pull's first message.
            $this->subscribe($client, 'app.extra');
            $server->releaseAnswers();
            $this->waitUntil(static fn(): bool => self::messagesContaining($failures, 'maximum subscriptions exceeded') !== []);
            delay(0.02);
            self::assertFalse($fetching->isComplete(), 'the fetch waits on for its second message, its inbox confirmed');

            $server->pushFrame(implode('', self::dataFrameOn($server, $this->firstPull(), 'late')));
            [$payloads, $error] = self::settle($fetching);
            self::assertNull($error, 'the fetch got its messages: ' . self::describe($error));
            self::assertSame(['m1', 'late'], $payloads);
        } finally {
            $stop->cancel();
            $loop->await();
        }
    }

    /**
     * The pinned, documented behaviour: two inboxes unconfirmed at once cannot be told apart by an -ERR that names
     * none, so both are treated as rejected. The server never answers the PINGs here, so the first fetch's inbox,
     * which the server took, stays unconfirmed while its pull waits; the second fetch's SUB is rejected, the loop's
     * read meeting the -ERR, and both fetches fail at once with the limit error. Both inboxes are unsubscribed, which
     * frees the slot the first held, and a fetch then gets its message, its inbox confirmed by the delivery.
     */
    public function testTwoInboxesUnconfirmedAtOnceAreBothTreatedAsRejected(): void
    {
        $server = $this->pullServer(limit: 2);
        $client = $this->connect($server);
        $server->answerPings = false;
        $this->subscribe($client, 'fill.a');
        $this->answerPulls = false;
        [$stop, $loop] = $this->startReadLoop($client);

        try {
            $js = $client->jetStream();
            $first = self::startOperation('fetchBatch', $js);
            $this->waitUntil(fn(): bool => count($this->pulls) === 1);
            self::assertCount(2, $server->heldSubscriptions(), "the server took the first fetch's inbox");

            $start = hrtime(true);
            $second = self::startOperation('fetchBatch', $js);
            [, $firstError] = self::settle($first);
            [, $secondError] = self::settle($second);
            $elapsed = self::secondsSince($start);

            self::assertInstanceOf(ConnectionException::class, $firstError, 'the first fetch ended with ' . self::describe($firstError));
            self::assertStringContainsString(self::LIMIT, $firstError->getMessage());
            self::assertInstanceOf(ConnectionException::class, $secondError, 'the second fetch ended with ' . self::describe($secondError));
            self::assertStringContainsString(self::LIMIT, $secondError->getMessage());
            self::assertLessThan(1.0, $elapsed, sprintf('both fetches ended after %.3f s, with 3 s deadlines', $elapsed));
            self::assertCount(2, $server->controlLines('UNSUB'), 'both inboxes were unsubscribed');
            self::assertSame(['fill.a'], array_values($server->heldSubscriptions()), "the first inbox's slot is free again");

            $this->answerPulls = true;
            [$payloads, $error] = self::settle(self::startOperation('fetchBatch', $js));
            self::assertNull($error, 'the next fetch got its message: ' . self::describe($error));
            self::assertSame(['m1'], $payloads);
        } finally {
            $stop->cancel();
            $loop->await();
        }
    }

    /**
     * The pinned, documented behaviour with the shared reply inbox: a request's inbox and a fetch's are unconfirmed
     * together (no PING is answered), another subscription is rejected for the limit, and the loop's read meets the
     * -ERR: the reply inbox is dropped and the request fails with the dropped-inbox error, the fetch's inbox is treated
     * as rejected and the fetch fails with the limit error, both at once.
     */
    public function testAnInboxUnconfirmedTogetherWithTheReplyInboxIsTreatedAsRejectedWithIt(): void
    {
        $server = $this->pullServer(limit: 2);
        $client = $this->connect($server);
        $server->answerPings = false;
        $this->answerPulls = false;
        [$stop, $loop] = $this->startReadLoop($client);

        try {
            $request = $client->request('svc.held', 'x', 3_000)->map(static fn(NatsMessage $message): array => [$message->payload]);
            $request->ignore();
            $this->waitUntil(static fn(): bool => $server->controlLines('PUB svc.held') !== []);
            $fetching = self::startOperation('fetchBatch', $client->jetStream());
            $this->waitUntil(fn(): bool => count($this->pulls) === 1);
            self::assertCount(2, $server->heldSubscriptions(), 'the server took the reply inbox and the fetch inbox');

            $start = hrtime(true);
            $this->subscribe($client, 'app.extra');
            [, $requestError] = self::settle($request);
            [, $fetchError] = self::settle($fetching);
            $elapsed = self::secondsSince($start);

            self::assertInstanceOf(ConnectionException::class, $requestError, 'the request ended with ' . self::describe($requestError));
            self::assertStringContainsString(self::MUX_DROPPED, $requestError->getMessage());
            self::assertInstanceOf(ConnectionException::class, $fetchError, 'the fetch ended with ' . self::describe($fetchError));
            self::assertStringContainsString(self::LIMIT, $fetchError->getMessage());
            self::assertLessThan(1.0, $elapsed, sprintf('the request and the fetch ended after %.3f s, with 3 s deadlines', $elapsed));
        } finally {
            $stop->cancel();
            $loop->await();
        }
    }

    /**
     * Guard, no change expected: a subscription of the application's rejected for the limit behaves as before. The
     * loop's read fails with the -ERR, the subscription stays registered - the client cannot tell which SUB the -ERR
     * answered - and no UNSUB is written for it; the connection goes on, and once the slots are free a fetch works.
     */
    public function testAUserSubscriptionRejectedByTheLimitBehavesAsBefore(): void
    {
        $server = $this->pullServer(limit: 1);
        $client = $this->connect($server);
        $fillSid = $this->subscribe($client, 'fill.a');
        [$stop, $loop, $failures] = $this->startReadLoop($client);

        try {
            $extraSid = $this->subscribe($client, 'app.extra');
            $this->waitUntil(static fn(): bool => self::messagesContaining($failures, 'maximum subscriptions exceeded') !== []);
            delay(0.02);

            self::assertTrue($client->isSubscriptionActive($extraSid), 'the subscription stays registered');
            self::assertSame([], $server->controlLines('UNSUB'), 'no UNSUB was written');
            self::assertSame(ConnectionState::Open, $client->state());

            $client->unsubscribe($fillSid)->await();
            $client->unsubscribe($extraSid)->await();
            [$payloads, $error] = self::settle(self::startOperation('fetchBatch', $client->jetStream()));
            self::assertNull($error, 'the fetch got its message: ' . self::describe($error));
            self::assertSame(['m1'], $payloads);
        } finally {
            $stop->cancel();
            $loop->await();
        }
    }

    /** @return iterable<string, array{string, string, bool}> */
    public static function inboxesByPermissions(): iterable
    {
        yield 'a pull fetch, its own read' => ['fetchBatch', '_INBOX.JS.FETCH', false];
        yield 'a pull fetch, an application loop reading' => ['fetchBatch', '_INBOX.JS.FETCH', true];
        yield 'a Direct Get batch, its own read' => ['directGetBatch', '_INBOX.JS.DGET', false];
        yield 'a Direct Get batch, an application loop reading' => ['directGetBatch', '_INBOX.JS.DGET', true];
    }

    /**
     * The rejection handler of a guarded inbox keeps working for a permissions violation naming it, and now fails the
     * operation at once: the server denies the inbox prefix, the -ERR names the inbox (the server keeps the connection
     * open and the read does not fail), and the operation fails within 1 s with an error quoting the violation, where
     * it used to wait out its deadline. The inbox is unsubscribed and the connection stays Open. Whether the -ERR is
     * met by the operation's own read or by an application loop's: in the latter case the loop's read ends the
     * operation's wait for the socket, and the operation finds the rejection recorded when it looks again.
     */
    #[DataProvider('inboxesByPermissions')]
    public function testAPermissionsViolationNamingTheInboxFailsTheOperationAtOnce(string $operation, string $prefix, bool $anotherReads): void
    {
        $server = $this->pullServer();
        $server->denied = [$prefix];
        $recorder = new LifecycleRecorder();
        $client = $this->connect($server, recorder: $recorder);
        $stop = new DeferredCancellation();
        $loop = Future::complete();
        if ($anotherReads) {
            [$stop, $loop] = $this->startReadLoop($client);
        }

        try {
            $start = hrtime(true);
            [, $error] = self::settle(self::startOperation($operation, $client->jetStream()));
            $elapsed = self::secondsSince($start);

            self::assertInstanceOf(ConnectionException::class, $error, sprintf('%s ended after %.3f s with %s', $operation, $elapsed, self::describe($error)));
            self::assertStringContainsString('the server rejected its reply-inbox subscription "' . $prefix . '.', $error->getMessage());
            self::assertStringContainsString('Permissions Violation for Subscription to', $error->getMessage());
            self::assertLessThan(1.0, $elapsed, sprintf('%s failed after %.3f s, with a 3 s deadline', $operation, $elapsed));
            self::assertNotSame([], $recorder->errorsContaining('Permissions Violation'), 'the violation was reported as well');
            $sid = self::lastSidOf($server->controlLines('SUB ' . $prefix));
            self::assertContains('UNSUB ' . $sid, $server->controlLines('UNSUB'), 'the inbox was unsubscribed');
            self::assertSame(ConnectionState::Open, $client->state());
        } finally {
            $stop->cancel();
            $loop->await();
        }
    }

    /**
     * A fetch in flight across a reconnect whose replay of its inbox the new server rejects for the limit: the replay
     * writes a PING behind the replayed SUB, the -ERR comes ahead of its PONG and the recovery's own read meets it. The
     * fetch, waiting for the recovery, fails at once with the limit error, within 1 s of the drop, where it used to read
     * on after the recovery to its 3 s deadline; the recovery completes, the connection Open, with the -ERR reported. Once
     * a slot is free a retry gets its message, and the UNSUB the rejected inbox owed went out on the new connection.
     */
    public function testAFetchWhoseReplayedInboxIsRejectedByTheNewServerFailsWithTheLimitError(): void
    {
        $server = $this->pullServer(limit: 2);
        $recorder = new LifecycleRecorder();
        $client = $this->connect($server, reconnect: true, recorder: $recorder);
        // Replayed ahead of the inbox, so it keeps the one slot left after the reconnect.
        $appSid = $this->subscribe($client, 'app.one');
        $this->answerPulls = false;
        $js = $client->jetStream();
        $fetching = self::startOperation('fetchBatch', $js);
        $this->waitUntil(fn(): bool => count($this->pulls) === 1);
        $inboxSid = self::lastSidOf($server->controlLines('SUB _INBOX.JS.FETCH'));

        $server->limit = 1;
        $server->dropConnection();
        $start = hrtime(true);
        [, $error] = self::settle($fetching);
        $elapsed = self::secondsSince($start);

        self::assertInstanceOf(ConnectionException::class, $error, sprintf('the fetch ended after %.3f s with %s', $elapsed, self::describe($error)));
        self::assertStringContainsString(self::LIMIT, $error->getMessage());
        self::assertLessThan(1.0, $elapsed, sprintf('the fetch failed after %.3f s, with a 3 s deadline', $elapsed));
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Open);
        self::assertSame(1, $server->epoch());
        self::assertNotSame([], $recorder->errorsContaining('maximum subscriptions exceeded'), 'the replayed inbox SUB was rejected');
        self::assertSame(['app.one'], array_values($server->heldSubscriptions()));

        $client->unsubscribe($appSid)->await();
        $this->answerPulls = true;
        [$payloads, $error] = self::settle(self::startOperation('fetchBatch', $js));
        self::assertNull($error, 'the retry got its message: ' . self::describe($error));
        self::assertSame(['m1'], $payloads);
        self::assertContains('UNSUB ' . $inboxSid, $server->controlLines('UNSUB', epoch: 1), 'the rejected inbox was unsubscribed on the new connection');
    }

    /**
     * A fetch whose inbox SUB write fails - the socket dies while the write is held up - and whose replay the new
     * server rejects for the limit (the limit lowered to one, app.one replayed first): the replay's drain reads the
     * -ERR, records the rejection and drops the inbox, and the subscribe the fetch still waits in reports it, where it
     * used to report a closed connection for a connection that is Open. The fetch fails within 1 s of the drop with the
     * limit error naming its inbox, the recovery completes Open with the -ERR reported, the rejected inbox is
     * unsubscribed once on the new connection, by the replay, nothing of its guard is left behind, and a retry once a
     * slot is free gets its message, with no second UNSUB for the rejected inbox.
     */
    public function testAFetchWhoseSubWriteFailedAndWhoseReplayIsRejectedFailsWithTheLimitError(): void
    {
        $server = $this->pullServer(limit: 2);
        $recorder = new LifecycleRecorder();
        $client = $this->connect($server, reconnect: true, recorder: $recorder);
        $connection = self::connectionOf($client);
        // Replayed ahead of the inbox, so it keeps the one slot left after the reconnect.
        $appSid = $this->subscribe($client, 'app.one');
        $this->answerPulls = false;
        $js = $client->jetStream();

        $server->stallNextWriteContaining('SUB _INBOX.JS.FETCH', 5.0);
        $fetching = self::startOperation('fetchBatch', $js);
        // The inbox is registered, unconfirmed, in the step that hands its SUB write to the socket, where it is held up.
        $this->waitUntil(static fn(): bool => self::privateArray($connection, 'unconfirmedSids') !== []);
        self::assertSame([], $server->controlLines('SUB _INBOX.JS.FETCH'), 'the SUB write is held up');

        $server->limit = 1;
        $start = hrtime(true);
        $server->dropConnection();
        [, $error] = self::settle($fetching);
        $elapsed = self::secondsSince($start);

        self::assertInstanceOf(ConnectionException::class, $error, sprintf('the fetch ended after %.3f s with %s', $elapsed, self::describe($error)));
        self::assertStringContainsString('JetStream pull fetch failed', $error->getMessage());
        self::assertStringContainsString(self::LIMIT, $error->getMessage());
        self::assertStringNotContainsString('the connection was closed', $error->getMessage());
        self::assertLessThan(1.0, $elapsed, sprintf('the fetch failed after %.3f s', $elapsed));
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Open);
        self::assertSame(1, $server->epoch());
        self::assertNotSame([], $recorder->errorsContaining('maximum subscriptions exceeded'), 'the replayed inbox SUB was rejected');
        self::assertSame(['app.one'], array_values($server->heldSubscriptions()));
        $inboxSid = self::lastSidOf($server->controlLines('SUB _INBOX.JS.FETCH', epoch: 1));
        self::assertSame(['UNSUB ' . $inboxSid], $server->controlLines('UNSUB', epoch: 1), 'the rejected inbox was unsubscribed once, by the replay');
        self::assertGuardStateEmpty($connection, 'after the fetch failed');

        $client->unsubscribe($appSid)->await();
        $this->answerPulls = true;
        [$payloads, $error] = self::settle(self::startOperation('fetchBatch', $js));
        self::assertNull($error, 'the retry got its message: ' . self::describe($error));
        self::assertSame(['m1'], $payloads);
        self::assertCount(1, array_keys($server->controlLines('UNSUB', epoch: 1), 'UNSUB ' . $inboxSid, true), 'no second UNSUB for the rejected inbox');
    }

    /**
     * Guard, passes on both (#206 kept it): the pull pipeline's twin of the previous test. A run whose inbox SUB write
     * fails - the socket dies while the write is held up - and whose replay the new server rejects for the limit (the
     * limit lowered to one, app.one replayed first): the subscribe the run still waits in reports the rejection, and
     * handle() fails within 1 s of the drop with the JetStreamException saying the inbox may have been rejected for the
     * limit, the subscribe's own error as its previous one, without writing a pull. The recovery completes Open with
     * the -ERR reported, and the new connection holds only app.one.
     */
    public function testAPullConsumerWhoseSubWriteFailedAndWhoseReplayIsRejectedFailsWithTheLimitError(): void
    {
        $server = $this->pullServer(limit: 2);
        $recorder = new LifecycleRecorder();
        $client = $this->connect($server, reconnect: true, recorder: $recorder);
        $connection = self::connectionOf($client);
        // Replayed ahead of the inbox, so it keeps the one slot left after the reconnect.
        $this->subscribe($client, 'app.one');
        $this->answerPulls = false;

        $server->stallNextWriteContaining('SUB _INBOX.JS.PULL', 5.0);
        $run = $client->jetStream()->pullConsumer('S', 'C')->setExpiresMs(2_000)->setBatching(1)->setDepth(1)->handle(static function (): void {});
        $run->ignore();
        // The inbox is registered, unconfirmed, in the step that hands its SUB write to the socket, where it is held up.
        $this->waitUntil(static fn(): bool => self::privateArray($connection, 'unconfirmedSids') !== []);
        self::assertSame([], $server->controlLines('SUB _INBOX.JS.PULL'), 'the SUB write is held up');

        $server->limit = 1;
        $start = hrtime(true);
        $server->dropConnection();
        try {
            $run->await(new TimeoutCancellation(2.0));
            self::fail('expected the run to fail: the new server rejected its replayed inbox');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('Pull consumer reply inbox "_INBOX.JS.PULL.', $e->getMessage());
            self::assertStringContainsString('may have been rejected by the server because the connection is at its subscription limit', $e->getMessage());
            self::assertInstanceOf(ConnectionException::class, $e->getPrevious(), 'the subscribe reported the rejection');
        }
        $elapsed = self::secondsSince($start);

        self::assertLessThan(1.0, $elapsed, sprintf('the run failed after %.3f s', $elapsed));
        self::assertSame([], $this->pulls->getArrayCopy(), 'no pull was written');
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Open);
        self::assertSame(1, $server->epoch());
        self::assertNotSame([], $recorder->errorsContaining('maximum subscriptions exceeded'), 'the replayed inbox SUB was rejected');
        self::assertSame(['app.one'], array_values($server->heldSubscriptions()));
    }

    /**
     * The pull pipeline's replay: a run in flight across a reconnect whose replay of its inbox the new server rejects
     * for the limit (the limit lowered to one, app.one replayed first). handle() fails within 1 s of the drop with the
     * JetStreamException saying the inbox may have been rejected for the limit, where the engine used to re-issue its
     * lost pulls on the dead inbox and retire them at their deadlines (the test waits at most 2 s); the recovery
     * completes Open with the -ERR reported, and the inbox's UNSUB went out on the new connection, which holds only
     * app.one.
     */
    public function testAPullConsumerWhoseReplayedInboxIsRejectedByTheNewServerFailsWithTheLimitError(): void
    {
        $server = $this->pullServer(limit: 2);
        $recorder = new LifecycleRecorder();
        $client = $this->connect($server, reconnect: true, recorder: $recorder);
        // Replayed ahead of the inbox, so it keeps the one slot left after the reconnect.
        $this->subscribe($client, 'app.one');
        $this->answerPulls = false;
        $run = $client->jetStream()->pullConsumer('S', 'C')->setExpiresMs(2_000)->setBatching(1)->setDepth(1)->handle(static function (): void {});
        $run->ignore();
        $this->waitUntil(fn(): bool => count($this->pulls) === 1);
        $inboxSid = self::lastSidOf($server->controlLines('SUB _INBOX.JS.PULL'));

        $server->limit = 1;
        $start = hrtime(true);
        $server->dropConnection();
        try {
            $run->await(new TimeoutCancellation(2.0));
            self::fail('expected the run to fail: the new server rejected its inbox');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('Pull consumer reply inbox "_INBOX.JS.PULL.', $e->getMessage());
            self::assertStringContainsString('may have been rejected by the server because the connection is at its subscription limit', $e->getMessage());
        }
        $elapsed = self::secondsSince($start);

        self::assertLessThan(1.0, $elapsed, sprintf('the run failed after %.3f s, with a 3 s pull deadline', $elapsed));
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Open);
        self::assertSame(1, $server->epoch());
        self::assertNotSame([], $recorder->errorsContaining('maximum subscriptions exceeded'), 'the replayed inbox SUB was rejected');
        self::assertContains('UNSUB ' . $inboxSid, $server->controlLines('UNSUB', epoch: 1), 'the rejected inbox was unsubscribed on the new connection');
        self::assertSame(['app.one'], array_values($server->heldSubscriptions()));
    }

    /**
     * Guard, white-box: nothing of an inbox's guard is left behind once its operation ends - with its message (the PONG
     * behind the SUB confirmed the inbox), with a timeout (an empty pull, a 408), or with the limit rejection. The
     * guarded sids, the unconfirmed sids, the UNSUBs owed, the slow-consumer exemptions and the rejection handlers are
     * all empty afterwards.
     */
    public function testNoGuardStateIsLeftBehindOnceAnOperationEnds(): void
    {
        $server = $this->pullServer(limit: 2);
        $client = $this->connect($server);
        $connection = self::connectionOf($client);
        $js = $client->jetStream();

        self::assertSame(['m1'], self::payloads($js->fetchBatch('S', 'C', 1, 2_000)->await()));
        self::assertGuardStateEmpty($connection, 'after a fetch got its message');

        $this->answerPulls = false;
        try {
            $js->fetchBatch('S', 'C', 1, 30)->await();
            self::fail('expected the fetch to time out: the server holds its pull');
        } catch (JetStreamException $e) {
            self::assertSame(408, $e->getCode());
        }
        self::assertGuardStateEmpty($connection, 'after a fetch timed out');

        $this->subscribe($client, 'fill.a');
        $this->subscribe($client, 'fill.b');
        try {
            $js->fetchBatch('S', 'C', 1, 2_000)->await();
            self::fail('expected the fetch to fail: the server rejected its inbox');
        } catch (ConnectionException $e) {
            self::assertStringContainsString(self::SERVER_ERR, $e->getMessage());
        }
        self::assertGuardStateEmpty($connection, 'after a fetch whose inbox was rejected');
    }

    /**
     * Guard, white-box: dropping a subscription's state removes its guard with it, and leaves the other guarded sids
     * alone.
     */
    public function testDroppingASubscriptionsStateRemovesItsGuard(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport());
        /** @var DeferredFuture<null> $slot */
        $slot = new DeferredFuture();
        self::setPrivate($connection, 'guardedSids', [7 => true, 8 => true]);
        self::setPrivate($connection, 'unconfirmedSids', [7 => $slot]);

        (new \ReflectionMethod(NatsConnection::class, 'dropSubscriptionState'))->invoke($connection, 7);

        self::assertSame([8 => true], self::privateArray($connection, 'guardedSids'));
        self::assertSame([], self::privateArray($connection, 'unconfirmedSids'));
    }

    /**
     * Guard, white-box: the slow-consumer exemption and the rejection-handler mark leave a sid that is no longer
     * registered alone - another fiber's read can have dropped a rejected inbox between its SUB write and those calls,
     * which the operation makes once the write completes - so that nothing is recorded for a sid that is gone.
     */
    public function testTheMarksLeaveASidThatIsNoLongerRegisteredAlone(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport());

        $connection->markSubscriptionUnbounded(7);
        $connection->markSubscriptionRejectionHandler(7, static function (): void {});

        self::assertSame([], self::privateArray($connection, 'unboundedSids'));
        self::assertSame([], self::privateArray($connection, 'subscriptionRejectionHandlers'));
    }

    /**
     * A server that models the JetStream pull API behind the limit: it answers a pull (CONSUMER.MSG.NEXT) with one
     * message, delivered as a real server does - on the stored message's subject with a $JS.ACK reply, on the
     * subscription that covers the pull's reply subject - or holds it ({@see $answerPulls}), and records every pull's
     * reply subject ({@see $pulls}); a batched Direct Get with its end-of-batch marker; "svc.echo" with an echo;
     * "svc.held" never. Nothing arrives on an inbox the server rejected.
     */
    private function pullServer(int $limit = PHP_INT_MAX): SubscriptionLimitServer
    {
        $server = new SubscriptionLimitServer();
        $server->limit = $limit;
        $server->responder = function (string $subject, ?string $replyTo, string $payload) use ($server): array {
            if ($replyTo === null || $subject === 'svc.held') {
                return [];
            }

            if ($subject === self::PULL_SUBJECT) {
                $this->pulls[] = $replyTo;

                return $this->answerPulls ? self::dataFrameOn($server, $replyTo, 'm1') : [];
            }

            if ($subject === self::DIRECT_GET_SUBJECT) {
                return self::endOfBatchFrameOn($server, $replyTo);
            }

            return $server->replyFrame($replyTo, 'echo:' . $payload);
        };

        return $server;
    }

    /**
     * A pull's delivery, as a real server makes it: on the stored message's subject, with a $JS.ACK reply, on the sid
     * of the subscription the live session holds for $replyTo - the fetch's exact inbox, or the pipeline's "<base>.*" -
     * or no frame when it holds none.
     *
     * @return list<string>
     */
    private static function dataFrameOn(SubscriptionLimitServer $server, string $replyTo, string $payload): array
    {
        $sid = self::sidHeldFor($server, $replyTo);

        return $sid === null
            ? []
            : [sprintf("MSG evt.s %d \$JS.ACK.S.C.1.1.1.0.0 %d\r\n%s\r\n", $sid, strlen($payload), $payload)];
    }

    /**
     * A batched Direct Get's end-of-batch marker (a 204 status) on the sid the live session holds for $replyTo, or no
     * frame when it holds none.
     *
     * @return list<string>
     */
    private static function endOfBatchFrameOn(SubscriptionLimitServer $server, string $replyTo): array
    {
        $sid = self::sidHeldFor($server, $replyTo);
        if ($sid === null) {
            return [];
        }

        $headers = "NATS/1.0 204 EOB\r\n\r\n";

        return [sprintf("HMSG %s %d %d %d\r\n%s\r\n", $replyTo, $sid, strlen($headers), strlen($headers), $headers)];
    }

    /** The sid of the subscription the live session holds for $subject: an exact one, or a "<base>.*" wildcard covering it. */
    private static function sidHeldFor(SubscriptionLimitServer $server, string $subject): ?int
    {
        foreach ($server->heldSubscriptions() as $sid => $held) {
            if ($held === $subject) {
                return $sid;
            }

            if (str_ends_with($held, '.*')) {
                $base = substr($held, 0, -1);
                if (str_starts_with($subject, $base) && !str_contains(substr($subject, strlen($base)), '.')) {
                    return $sid;
                }
            }
        }

        return null;
    }

    private function connect(SubscriptionLimitServer $server, bool $reconnect = false, ?LifecycleRecorder $recorder = null): NatsClient
    {
        $client = new NatsClient(new NatsOptions(
            connectTimeoutMs: 500,
            reconnectEnabled: $reconnect,
            maxReconnectAttempts: 3,
            reconnectDelayMs: 1,
            reconnectMaxDelayMs: 1,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
            errorListener: $recorder?->errorListener(),
            waitForReconnect: true,
        ), $server);
        $this->opened[] = $client;
        $client->connect()->await();

        return $client;
    }

    /** The reply subject of the first pull the scripted server received. */
    private function firstPull(): string
    {
        $pulls = $this->pulls->getArrayCopy();
        self::assertArrayHasKey(0, $pulls, 'the server received no pull');

        return $pulls[0];
    }

    private function subscribe(NatsClient $client, string $subject): int
    {
        return $client->subscribe($subject, static function (): void {})->await();
    }

    /**
     * An application loop that keeps a read on the socket, as a consumer's does, and reads on past whatever a read
     * throws, collecting it: an -ERR the server keeps the connection open for fails the read that brings it. Stop it
     * with the cancellation, then await the future.
     *
     * @return array{DeferredCancellation, Future<void>, \ArrayObject<int, \Throwable>}
     */
    private function startReadLoop(NatsClient $client): array
    {
        /** @var \ArrayObject<int, \Throwable> $failures */
        $failures = new \ArrayObject();
        $stop = new DeferredCancellation();
        $loop = async(static function () use ($client, $stop, $failures): void {
            while (!$stop->isCancelled()) {
                try {
                    $client->processIncoming($stop->getCancellation())->await();
                } catch (CancelledException) {
                    return;
                } catch (\Throwable $e) {
                    $failures[] = $e;
                    delay(0.001);
                }
            }
        });
        // Let the loop take the read first.
        delay(0.01);

        return [$stop, $loop, $failures];
    }

    /**
     * The operation, started: a pull fetch of one message as fetchBatch() or fetchNext(), or a Direct Get batch, each
     * with a 2 s expiry; its payloads.
     *
     * @return Future<list<string>>
     */
    private static function startOperation(string $operation, JetStreamContext $js): Future
    {
        $result = match ($operation) {
            'fetchNext' => $js->fetchNext('S', 'C', 2_000)->map(static fn(NatsMessage $message): array => [$message->payload]),
            'directGetBatch' => $js->directGetBatch('S', ['seq' => 1, 'batch' => 1], 2_000)->map(self::payloads(...)),
            default => $js->fetchBatch('S', 'C', 1, 2_000)->map(self::payloads(...)),
        };
        $result->ignore();

        return $result;
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

    /**
     * @param \ArrayObject<int, \Throwable> $failures
     * @return list<string>
     */
    private static function messagesContaining(\ArrayObject $failures, string $needle): array
    {
        $messages = [];
        foreach ($failures as $failure) {
            if (str_contains($failure->getMessage(), $needle)) {
                $messages[] = $failure->getMessage();
            }
        }

        return $messages;
    }

    /**
     * The sid of the last SUB among $lines, control lines such as "SUB _INBOX.JS.FETCH.abc 3".
     *
     * @param list<string> $lines
     */
    private static function lastSidOf(array $lines): int
    {
        self::assertNotSame([], $lines, 'no such SUB was written');
        $parts = explode(' ', $lines[count($lines) - 1]);

        return (int) $parts[count($parts) - 1];
    }

    /** @param \Closure(): bool $condition */
    private function waitUntil(\Closure $condition): void
    {
        $deadline = hrtime(true) + 2_000_000_000;
        while (!$condition()) {
            if (hrtime(true) > $deadline) {
                self::fail('condition not reached within 2 s');
            }

            delay(0.001);
        }
    }

    private static function secondsSince(int $start): float
    {
        return (hrtime(true) - $start) / 1e9;
    }

    /**
     * @param array<array-key, NatsMessage> $messages
     * @return list<string>
     */
    private static function payloads(array $messages): array
    {
        return array_values(array_map(static fn(NatsMessage $message): string => $message->payload, $messages));
    }

    private static function connectionOf(NatsClient $client): NatsConnection
    {
        $connection = (new \ReflectionProperty(NatsClient::class, 'connection'))->getValue($client);
        self::assertInstanceOf(NatsConnection::class, $connection);

        return $connection;
    }

    /** @return array<array-key, mixed> */
    private static function privateArray(NatsConnection $connection, string $property): array
    {
        $value = (new \ReflectionProperty(NatsConnection::class, $property))->getValue($connection);
        self::assertIsArray($value);

        return $value;
    }

    /** @param array<array-key, mixed> $value */
    private static function setPrivate(NatsConnection $connection, string $property, array $value): void
    {
        (new \ReflectionProperty(NatsConnection::class, $property))->setValue($connection, $value);
    }

    private static function assertGuardStateEmpty(NatsConnection $connection, string $when): void
    {
        foreach (['guardedSids', 'unconfirmedSids', 'guardedSidsToRelease', 'unboundedSids', 'subscriptionRejectionHandlers'] as $property) {
            self::assertSame([], self::privateArray($connection, $property), $property . ' ' . $when);
        }
    }
}
