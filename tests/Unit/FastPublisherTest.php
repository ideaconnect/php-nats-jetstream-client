<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\FastBatchException;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Exception\SubscriptionLimitException;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\Exception\UnsupportedFeatureException;
use IDCT\NATS\JetStream\FastPublisher;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\OwnsTestResources;
use IDCT\NATS\Tests\Support\ScriptedFastIngestServer;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Amp\async;
use function Amp\delay;

/**
 * The fast-ingest batch publisher (ADR-50, #12) against a scripted server. The server replies are the ones
 * nats-server 2.14.10 and 2.15.3 sent for the same message sequences, word for word (the batch ids aside).
 */
final class FastPublisherTest extends TestCase
{
    use OwnsTestResources;

    private const INFO_2_14 = 'INFO {"server_id":"S1","server_name":"n1","version":"2.14.10","jetstream":true,"max_payload":1048576,"headers":true,"proto":1}';

    protected function tearDown(): void
    {
        $this->releaseOwnedResources();
    }

    /**
     * A connected client on a FakeTransport, with the scripted fast-ingest server attached after the handshake.
     *
     * @param \Closure(array{subject:string,reply:string,flow:int,gap:string,seq:int,op:int,headers:string,payload:string}): list<string> $respond
     * @return array{NatsClient, ScriptedFastIngestServer, FakeTransport}
     */
    private function connect(\Closure $respond, string $info = self::INFO_2_14, string $inboxPrefix = '_INBOX', ?\Closure $errorListener = null): array
    {
        $transport = new FakeTransport([$info . "\r\n", "PONG\r\n"]);
        $client = $this->own(new NatsClient(new NatsOptions(
            inboxPrefix: $inboxPrefix,
            requestTimeoutMs: 2_000,
            pingIntervalSeconds: 0,
            errorListener: $errorListener,
        ), $transport));
        $client->connect()->await();

        return [$client, new ScriptedFastIngestServer($transport, $respond), $transport];
    }

    private static function ack(int $seq, int $msgs): string
    {
        return sprintf('{"type":"ack","seq":%d,"msgs":%d}', $seq, $msgs);
    }

    private static function pubAck(int $streamSeq, string $batchId, int $count): string
    {
        return sprintf('{"stream":"FI","seq":%d,"batch":"%s","count":%d}', $streamSeq, $batchId, $count);
    }

    /** Waits for a future-less fiber to get as far as it can (the scripted reads are non-blocking). */
    private static function settle(): void
    {
        delay(0.05);
    }

    /**
     * U1 (case A): a batch of 26 at flow 10. Every reply subject is `<inbox>.10.fail.<seq>.<op>.$FI`, start 0, appends
     * 1, commit 2; one SUB on `<inbox>.>` (an inbox of the batch's own, whose last token is the batch id), its UNSUB
     * after the commit, and the commit's PubAck with the batch id and count.
     */
    public function testABatchOf26AtFlow10UsesItsOwnInboxAndTheReplySubjectsOfTheProtocol(): void
    {
        $batchId = null;
        [$client, $server] = $this->connect(static function (array $m) use (&$batchId): array {
            return match (true) {
                $m['op'] === 0 => [self::ack(0, 10)],
                $m['op'] === 1 && $m['seq'] % 10 === 0 => [self::ack($m['seq'], 10)],
                $m['op'] === 2 => [self::pubAck(26, (string) $batchId, 26)],
                default => [],
            };
        });

        $fast = $client->jetStream()->fastPublisher(flow: 10);
        $batchId = $fast->batchId();
        self::assertMatchesRegularExpression('/^[0-9a-f]{24}$/', $batchId);

        for ($i = 1; $i <= 25; ++$i) {
            $ack = $fast->add('fi.events', 'm-' . $i)->await();
            self::assertSame($i, $ack->batchSequence);
        }

        $pubAck = $fast->commit('fi.events', 'm-26')->await();

        self::assertSame('FI', $pubAck->stream);
        self::assertSame(26, $pubAck->seq);
        self::assertSame($batchId, $pubAck->batchId);
        self::assertSame(26, $pubAck->batchCount);
        self::assertTrue($fast->isClosed());

        $inbox = '_INBOX.' . $batchId;
        self::assertSame(1, $server->subscribes, 'one subscription of its own per batch');
        self::assertSame($inbox . '.>', $server->subscription);
        self::assertSame([(string) $server->sid], $server->unsubscribes, 'the inbox is released once the batch is committed');

        $expected = [$inbox . '.10.fail.1.0.$FI'];
        for ($i = 2; $i <= 25; ++$i) {
            $expected[] = $inbox . '.10.fail.' . $i . '.1.$FI';
        }
        $expected[] = $inbox . '.10.fail.26.2.$FI';
        self::assertSame($expected, array_column($server->messages, 'reply'));
        self::assertSame('m-26', $server->messages[25]['payload']);
        self::assertSame('', $server->messages[0]['headers'], 'nothing travels in headers');
    }

    /**
     * U2: after message 1 nothing is written before the server's first reply (ADR-50: the client MUST wait for it).
     */
    public function testNothingAfterTheFirstMessageIsWrittenBeforeTheFirstAck(): void
    {
        [$client, $server] = $this->connect(static fn(array $m): array => []);
        $fast = $client->jetStream()->fastPublisher(flow: 10);

        $first = $fast->add('fi.events', 'm-1');
        self::settle();
        self::assertSame(['1.0'], $server->sequenceAndOperations());
        self::assertFalse($first->isComplete(), 'the first add() waits for the first reply');

        $server->reply($server->replySubject(1, 0), self::ack(0, 10));
        $ack = $first->await();
        self::assertSame(1, $ack->batchSequence);
        self::assertSame(0, $ack->ackSequence);

        $fast->add('fi.events', 'm-2')->await();
        self::assertSame(['1.0', '2.1'], $server->sequenceAndOperations());
    }

    /**
     * U3: flow 1 with 1 outstanding ack stalls message 2 until the ack for 2 arrives.
     */
    public function testFlowOneWithOneOutstandingAckStallsMessageTwo(): void
    {
        [$client, $server] = $this->connect(static fn(array $m): array => $m['op'] === 0 ? [self::ack(0, 1)] : []);
        $fast = $client->jetStream()->fastPublisher(flow: 1, maxOutstandingAcks: 1);
        $fast->add('fi.events', 'm-1')->await();

        $second = $fast->add('fi.events', 'm-2');
        self::settle();
        self::assertSame(['1.0', '2.1'], $server->sequenceAndOperations(), 'message 2 is sent, then waits');
        self::assertFalse($second->isComplete());

        $server->reply($server->replySubject(1, 0), self::ack(1, 1));
        self::settle();
        self::assertFalse($second->isComplete(), 'the ack for 1 does not cover message 2');

        $server->reply($server->replySubject(2, 1), self::ack(2, 1));
        self::assertSame(2, $second->await()->ackSequence);
    }

    /**
     * U3: flow 100 with 2 outstanding: messages 2 to 199 return without a read (the ack for 100, already on the
     * wire after message 100, is not seen), and message 200 waits for it.
     */
    public function testFlow100WithTwoOutstandingReturnsUpTo199WithoutAReadAndWaitsAt200(): void
    {
        [$client, $server] = $this->connect(static fn(array $m): array => match (true) {
            $m['op'] === 0 => [self::ack(0, 100)],
            $m['seq'] === 100 => [self::ack(100, 100)],
            default => [],
        });
        $fast = $client->jetStream()->fastPublisher();

        for ($i = 1; $i <= 199; ++$i) {
            self::assertSame(0, $fast->add('fi.events', 'm')->await()->ackSequence, 'message ' . $i . ' read nothing');
        }

        $ack = $fast->add('fi.events', 'm')->await();
        self::assertSame(200, $ack->batchSequence);
        self::assertSame(100, $ack->ackSequence, 'message 200 waited for the ack for 100');
    }

    /**
     * U3: message 200 of a flow-100 batch with 2 outstanding acks does not return until the ack for 100 arrives
     * (`<=`, not `<`: Orbit's WaitForAckOnExactBoundary).
     */
    public function testMessage200WaitsUntilTheAckFor100Arrives(): void
    {
        [$client, $server] = $this->connect(static fn(array $m): array => $m['op'] === 0 ? [self::ack(0, 100)] : []);
        $fast = $client->jetStream()->fastPublisher(flow: 100, maxOutstandingAcks: 2);

        for ($i = 1; $i <= 199; ++$i) {
            $fast->add('fi.events', 'm')->await();
        }

        $message200 = $fast->add('fi.events', 'm');
        self::settle();
        self::assertFalse($message200->isComplete(), 'message 200 is at the boundary: it waits');
        self::assertCount(200, $server->messages);

        $server->reply($server->replySubject(100, 1), self::ack(100, 100));
        self::assertSame(100, $message200->await()->ackSequence);
    }

    /**
     * U4 (case H): an ack that lowers `msgs` to 1 makes the next message wait, while every reply subject keeps the
     * flow asked for (`.10.fail.`).
     */
    public function testAnAckThatLowersTheFlowMakesTheNextMessageWaitAndTheReplySubjectKeepsTheAskedFlow(): void
    {
        [$client, $server] = $this->connect(static fn(array $m): array => $m['op'] === 0 ? [self::ack(0, 1)] : []);
        $fast = $client->jetStream()->fastPublisher(flow: 10, maxOutstandingAcks: 2);
        $fast->add('fi.events', 'm-1')->await();

        $second = $fast->add('fi.events', 'm-2');
        self::settle();
        self::assertFalse($second->isComplete(), 'with msgs 1 and 2 outstanding, message 2 waits');

        $server->reply($server->replySubject(1, 0), self::ack(1, 2));
        self::assertSame(1, $second->await()->ackSequence);

        $fast->add('fi.events', 'm-3')->await();
        foreach ($server->messages as $message) {
            self::assertStringContainsString('.10.fail.', $message['reply']);
            self::assertSame(10, $message['flow']);
        }
    }

    /**
     * U5: flow 10 with 2 outstanding and the ack for 20 lost: message 30 waits until the ack for 30 arrives, and
     * that ack alone releases it (an ack covers every earlier one).
     */
    public function testALaterAckCoversALostOne(): void
    {
        [$client, $server] = $this->connect(static fn(array $m): array => match (true) {
            $m['op'] === 0 => [self::ack(0, 10)],
            $m['seq'] === 10 => [self::ack(10, 10)],
            default => [],
        });
        $fast = $client->jetStream()->fastPublisher(flow: 10, maxOutstandingAcks: 2);

        for ($i = 1; $i <= 29; ++$i) {
            $fast->add('fi.events', 'm')->await();
        }

        $message30 = $fast->add('fi.events', 'm');
        self::settle();
        self::assertFalse($message30->isComplete());

        $server->reply($server->replySubject(30, 1), self::ack(30, 10));
        self::assertSame(30, $message30->await()->ackSequence);
    }

    /**
     * U6: with the acks held back, a ping at the highest batch sequence goes out after a third of the ack timeout;
     * its answer releases the stall, and the next message carries the sequence after it (a ping does not advance it).
     */
    public function testAStalledMessagePingsAtTheHighestSequenceWhichThePingDoesNotAdvance(): void
    {
        [$client, $server] = $this->connect(static fn(array $m): array => match ($m['op']) {
            0 => [self::ack(0, 1)],
            4 => [self::ack($m['seq'], 1)],
            1 => $m['seq'] === 3 ? [self::ack(3, 1)] : [],
            default => [],
        });
        $fast = $client->jetStream()->fastPublisher(flow: 1, maxOutstandingAcks: 1, ackTimeoutMs: 600);
        $fast->add('fi.events', 'm-1')->await();

        $startNs = hrtime(true);
        self::assertSame(2, $fast->add('fi.events', 'm-2')->await()->ackSequence);
        $elapsed = (hrtime(true) - $startNs) / 1e9;
        self::assertGreaterThanOrEqual(0.19, $elapsed, 'the ping goes out after a third of the ack timeout');

        $fast->add('fi.events', 'm-3')->await();

        self::assertSame(['1.0', '2.1', '2.4', '3.1'], $server->sequenceAndOperations());
        self::assertSame('fi.events', $server->messages[2]['subject']);
        self::assertSame('', $server->messages[2]['payload']);
    }

    /**
     * U6: with no ack at all, the stalled add() pings at a third and two thirds of the ack timeout, at the same
     * sequence, and gives up after the ack timeout in all: a TimeoutException with the ack sequence, the publisher
     * closed and its inbox released; a later add() sends nothing.
     */
    public function testAStallTimesOutAfterTheAckTimeoutInAll(): void
    {
        [$client, $server] = $this->connect(static fn(array $m): array => $m['op'] === 0 ? [self::ack(0, 1)] : []);
        $fast = $client->jetStream()->fastPublisher(flow: 1, maxOutstandingAcks: 1, ackTimeoutMs: 300);
        $fast->add('fi.events', 'm-1')->await();

        $startNs = hrtime(true);
        try {
            $fast->add('fi.events', 'm-2')->await();
            self::fail('expected a TimeoutException');
        } catch (TimeoutException $e) {
            self::assertStringContainsString('message 2 waited 300 ms for a flow ack; ack sequence 0', $e->getMessage());
        }
        $elapsed = (hrtime(true) - $startNs) / 1e9;
        self::assertGreaterThanOrEqual(0.29, $elapsed);
        self::assertLessThan(1.5, $elapsed);

        self::assertSame(['1.0', '2.1', '2.4', '2.4'], $server->sequenceAndOperations());
        self::assertTrue($fast->isClosed());
        self::assertSame([(string) $server->sid], $server->unsubscribes);

        try {
            $fast->add('fi.events', 'm-3')->await();
            self::fail('expected the closed publisher to refuse');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('is closed', $e->getMessage());
            self::assertInstanceOf(TimeoutException::class, $e->getPrevious());
        }
        self::assertCount(4, $server->messages);
    }

    /**
     * @return iterable<string, array{string, class-string<JetStreamException>, int, int|null, string}>
     */
    public static function firstReplyFailures(): iterable
    {
        yield '10205 batch publish is disabled' => [
            '{"error":{"code":400,"err_code":10205,"description":"batch publish is disabled"},"stream":"NB","seq":0}',
            JetStreamException::class, 400, 10205, 'batch publish is disabled',
        ];
        yield '10206 invalid pattern' => [
            '{"error":{"code":400,"err_code":10206,"description":"batch publish pattern is invalid"},"stream":"FI","seq":0}',
            JetStreamException::class, 400, 10206, 'batch publish pattern is invalid',
        ];
        yield '10207 invalid id' => [
            '{"error":{"code":400,"err_code":10207,"description":"batch publish ID is invalid"},"stream":"FI","seq":0}',
            JetStreamException::class, 400, 10207, 'batch publish ID is invalid',
        ];
        yield '10211 too many inflight' => [
            '{"error":{"code":429,"err_code":10211,"description":"batch publish too many inflight"},"stream":"FI","seq":0}',
            JetStreamException::class, 429, 10211, 'batch publish too many inflight',
        ];
        yield '503 no responders' => [
            'STATUS 503',
            JetStreamException::class, 503, null, 'No JetStream responder for subject fi.events',
        ];
        yield 'another status' => [
            'STATUS 500',
            JetStreamException::class, 500, null, 'the server answered a message to fi.events with status 500',
        ];
        yield 'a plain PubAck from a server that predates fast ingest' => [
            '{"stream":"NB","seq":1}',
            UnsupportedFeatureException::class, 0, null, 'stored the message as a plain publish (stream "NB", seq 1)',
        ];
    }

    /**
     * U7: each way the server refuses to start the batch fails the first add(): nothing more is sent, the inbox is
     * released, and a later add() sends nothing either.
     *
     * @param class-string<JetStreamException> $class
     */
    #[DataProvider('firstReplyFailures')]
    public function testAFirstReplyThatDoesNotStartTheBatchFailsTheFirstAdd(string $reply, string $class, int $code, ?int $errCode, string $message): void
    {
        [$client, $server] = $this->connect(static fn(array $m): array => $m['op'] === 0 ? [$reply] : []);
        $fast = $client->jetStream()->fastPublisher(flow: 10);

        try {
            $fast->add('fi.events', 'm-1')->await();
            self::fail('expected the first add() to fail');
        } catch (JetStreamException $e) {
            self::assertSame($class, $e::class);
            self::assertSame($code, $e->getCode());
            self::assertSame($errCode, $e->getErrCode());
            self::assertStringContainsString($message, $e->getMessage());
            if ($e instanceof UnsupportedFeatureException) {
                self::assertSame('allow_batched', $e->feature);
                self::assertSame('2.14', $e->requiredVersion);
                self::assertSame('2.14.10', $e->serverVersion);
            }
        }

        self::assertTrue($fast->isClosed());
        self::assertSame([(string) $server->sid], $server->unsubscribes);

        $this->expectException(JetStreamException::class);
        try {
            $fast->add('fi.events', 'm-2')->await();
        } finally {
            self::assertSame(['1.0'], $server->sequenceAndOperations());
        }
    }

    /**
     * U8 (case D): in gap mode `fail`, a gap notice and the PubAck that ends the batch at 3 come back on message 5.
     * The add() whose stall reads them throws a FastBatchException with that PubAck (the gap notice its previous),
     * and every later call throws with the same count, sending nothing.
     */
    public function testAGapInFailModeEndsTheBatchWithItsPubAck(): void
    {
        $batchId = '';
        [$client, $server] = $this->connect(static function (array $m) use (&$batchId): array {
            return match ($m['seq']) {
                1 => [self::ack(0, 2)],
                2 => [self::ack(2, 2)],
                5 => ['{"type":"gap","last_seq":4,"seq":5}', self::pubAck(34, $batchId, 3)],
                default => [],
            };
        });
        $fast = $client->jetStream()->fastPublisher(flow: 2, maxOutstandingAcks: 2);
        $batchId = $fast->batchId();

        for ($i = 1; $i <= 5; ++$i) {
            $fast->add('fi.events', 'm-' . $i)->await();
        }

        try {
            $fast->add('fi.events', 'm-6')->await();
            self::fail('expected the batch to have ended');
        } catch (FastBatchException $e) {
            self::assertSame(3, $e->pubAck?->batchCount);
            self::assertSame(34, $e->pubAck->seq);
            $gap = $e->getPrevious();
            self::assertInstanceOf(FastBatchException::class, $gap);
            self::assertSame(5, $gap->batchSequence);
            self::assertStringContainsString('messages 4 to 4 were lost', $gap->getMessage());
        }

        self::assertTrue($fast->isClosed());
        self::assertSame([(string) $server->sid], $server->unsubscribes);
        $written = count($server->messages);

        foreach ([fn() => $fast->add('fi.events', 'm-7')->await(), fn() => $fast->commit('fi.events', 'm-7')->await(), fn() => $fast->close()->await()] as $call) {
            try {
                $call();
                self::fail('expected the ended batch to refuse');
            } catch (FastBatchException $e) {
                self::assertSame(3, $e->pubAck?->batchCount);
            }
        }
        self::assertCount($written, $server->messages, 'nothing is sent after the batch ended');
    }

    /**
     * U8 (case J): in gap mode `fail`, a refused message (an `err` notice, 10071) ends the batch with the PubAck at
     * the message before it; the next add() throws with that count and sends nothing.
     */
    public function testARefusedMessageInFailModeEndsTheBatch(): void
    {
        $batchId = '';
        [$client, $server] = $this->connect(static function (array $m) use (&$batchId): array {
            return match ($m['seq']) {
                1 => [self::ack(0, 1)],
                2 => [
                    '{"type":"err","seq":2,"error":{"code":400,"err_code":10071,"description":"wrong last sequence: 45"}}',
                    self::pubAck(45, $batchId, 1),
                ],
                default => [],
            };
        });
        $fast = $client->jetStream()->fastPublisher(flow: 1, maxOutstandingAcks: 1);
        $batchId = $fast->batchId();
        $fast->add('fi.events', 'm-1')->await();

        try {
            $fast->add('fi.events', 'm-2', ['Nats-Expected-Last-Sequence' => '999999'])->await();
            self::fail('expected the batch to have ended');
        } catch (FastBatchException $e) {
            self::assertSame(1, $e->pubAck?->batchCount);
            $refused = $e->getPrevious();
            self::assertInstanceOf(FastBatchException::class, $refused);
            self::assertSame(10071, $refused->getErrCode());
            self::assertSame(400, $refused->getCode());
            self::assertSame(2, $refused->batchSequence);
        }

        self::assertStringContainsString('Nats-Expected-Last-Sequence:999999', $server->messages[1]['headers']);

        try {
            $fast->add('fi.events', 'm-3')->await();
            self::fail('expected the ended batch to refuse');
        } catch (FastBatchException $e) {
            self::assertSame(1, $e->pubAck?->batchCount);
        }
        self::assertSame(['1.0', '2.1'], $server->sequenceAndOperations());
    }

    /**
     * U9 (case E): in gap mode `ok` a gap notice reaches onError and the batch goes on; the gap notice does not move
     * the ack sequence (the ack for 4 after it does), and the commit returns count 7.
     */
    public function testAGapInOkModeReachesOnErrorAndTheBatchGoesOn(): void
    {
        $batchId = '';
        [$client, $server] = $this->connect(static function (array $m) use (&$batchId): array {
            return match ($m['seq']) {
                1 => [self::ack(0, 2)],
                2 => [self::ack(2, 2)],
                5 => ['{"type":"gap","last_seq":4,"seq":5}', self::ack(4, 2)],
                7 => [self::pubAck(40, $batchId, 7)],
                default => [],
            };
        });
        $notices = [];
        $fast = $client->jetStream()->fastPublisher(flow: 2, maxOutstandingAcks: 2, continueOnGap: true, onError: static function (FastBatchException $notice) use (&$notices): void {
            $notices[] = $notice;
        });
        $batchId = $fast->batchId();

        for ($i = 1; $i <= 5; ++$i) {
            $fast->add('fi.events', 'm-' . $i)->await();
        }
        $sixth = $fast->add('fi.events', 'm-6')->await();
        self::assertSame(4, $sixth->ackSequence, 'a gap notice never moves the ack sequence');

        self::assertCount(1, $notices);
        self::assertSame(5, $notices[0]->batchSequence);
        self::assertNull($notices[0]->pubAck);

        $pubAck = $fast->commit('fi.events', 'm-7')->await();
        self::assertSame(7, $pubAck->batchCount);
        self::assertSame('ok', $server->messages[0]['gap']);
    }

    /**
     * U9 (case K): in gap mode `ok` a refused message reaches onError, and the commit returns count 4 although 3
     * messages were stored: `batchCount` is the commit's batch sequence.
     */
    public function testARefusedMessageInOkModeReachesOnErrorAndTheCommitCountsTheSequence(): void
    {
        $batchId = '';
        [$client] = $this->connect(static function (array $m) use (&$batchId): array {
            return match ($m['seq']) {
                1 => [self::ack(0, 10)],
                2 => ['{"type":"err","seq":2,"error":{"code":400,"err_code":10071,"description":"wrong last sequence: 46"}}'],
                4 => [self::pubAck(48, $batchId, 4)],
                default => [],
            };
        });
        $notices = [];
        $fast = $client->jetStream()->fastPublisher(flow: 10, continueOnGap: true, onError: static function (FastBatchException $notice) use (&$notices): void {
            $notices[] = $notice;
        });
        $batchId = $fast->batchId();

        $fast->add('fi.events', 'm-1')->await();
        $fast->add('fi.events', 'm-2', ['Nats-Expected-Last-Sequence' => '999999'])->await();
        $fast->add('fi.events', 'm-3')->await();
        $pubAck = $fast->commit('fi.events', 'm-4')->await();

        self::assertSame(4, $pubAck->batchCount);
        self::assertSame(48, $pubAck->seq);
        self::assertCount(1, $notices);
        self::assertSame(10071, $notices[0]->getErrCode());
        self::assertSame(2, $notices[0]->batchSequence);
        self::assertStringContainsString('wrong last sequence: 46', $notices[0]->getMessage());
    }

    /**
     * What onError throws does not mask the call's result: it goes to the client's error listener.
     */
    public function testWhatOnErrorThrowsGoesToTheErrorListener(): void
    {
        $batchId = '';
        $reported = [];
        [$client] = $this->connect(static function (array $m) use (&$batchId): array {
            return match ($m['seq']) {
                1 => [self::ack(0, 10)],
                2 => ['{"type":"gap","last_seq":1,"seq":2}'],
                3 => [self::pubAck(3, $batchId, 3)],
                default => [],
            };
        }, errorListener: static function (\Throwable $e) use (&$reported): void {
            $reported[] = $e->getMessage();
        });
        $fast = $client->jetStream()->fastPublisher(continueOnGap: true, onError: static function (): void {
            throw new \RuntimeException('handler failed');
        });
        $batchId = $fast->batchId();

        $fast->add('fi.events', 'm-1')->await();
        $fast->add('fi.events', 'm-2')->await();
        self::assertSame(3, $fast->commit('fi.events', 'm-3')->await()->batchCount);
        self::assertContains('handler failed', $reported);
    }

    /**
     * A status reply to an appended message (no stream captures its subject) is a lost message: a notice, the batch
     * going on.
     */
    public function testANoRespondersStatusOnAnAppendedMessageIsANotice(): void
    {
        $batchId = '';
        [$client] = $this->connect(static function (array $m) use (&$batchId): array {
            return match ($m['seq']) {
                1 => [self::ack(0, 10)],
                2 => ['STATUS 503'],
                3 => [self::pubAck(2, $batchId, 3)],
                default => [],
            };
        });
        $notices = [];
        $fast = $client->jetStream()->fastPublisher(continueOnGap: true, onError: static function (FastBatchException $notice) use (&$notices): void {
            $notices[] = $notice;
        });
        $batchId = $fast->batchId();

        $fast->add('fi.events', 'm-1')->await();
        $fast->add('elsewhere', 'm-2')->await();
        self::assertSame(3, $fast->commit('fi.events', 'm-3')->await()->batchCount);

        self::assertCount(1, $notices);
        self::assertSame(503, $notices[0]->getCode());
        self::assertSame(2, $notices[0]->batchSequence);
    }

    /**
     * U10 (case B): close() writes an empty message with operation 3 to the subject of the batch's first message,
     * and returns the PubAck, count 5 after five adds (an EOB commit stores no message).
     */
    public function testCloseSendsAnEobCommitToTheBatchSubject(): void
    {
        $batchId = '';
        [$client, $server] = $this->connect(static function (array $m) use (&$batchId): array {
            return match ($m['op']) {
                0 => [self::ack(0, 100)],
                3 => [self::pubAck(31, $batchId, 5)],
                default => [],
            };
        });
        $fast = $client->jetStream()->fastPublisher();
        $batchId = $fast->batchId();

        $fast->add('fi.first', 'm-1')->await();
        for ($i = 2; $i <= 5; ++$i) {
            $fast->add('fi.other', 'm-' . $i)->await();
        }
        $pubAck = $fast->close()->await();

        self::assertSame(5, $pubAck->batchCount);
        self::assertSame($batchId, $pubAck->batchId);
        $eob = $server->messages[5];
        self::assertSame('fi.first', $eob['subject']);
        self::assertSame('', $eob['payload']);
        self::assertSame(3, $eob['op']);
        self::assertSame(6, $eob['seq']);
        self::assertStringEndsWith('.100.fail.6.3.$FI', $eob['reply']);
        self::assertTrue($fast->isClosed());
        self::assertSame([(string) $server->sid], $server->unsubscribes);
    }

    /**
     * Case M: a commit with no add() before it is a batch of one, operation 2 at sequence 1.
     */
    public function testACommitWithoutAnAddIsABatchOfOne(): void
    {
        $batchId = '';
        [$client, $server] = $this->connect(static function (array $m) use (&$batchId): array {
            return [self::pubAck(49, $batchId, 1)];
        });
        $fast = $client->jetStream()->fastPublisher();
        $batchId = $fast->batchId();

        self::assertSame(1, $fast->commit('fi.events', 'only')->await()->batchCount);
        self::assertSame(['1.2'], $server->sequenceAndOperations());
        self::assertSame(1, $server->subscribes);
        self::assertSame([(string) $server->sid], $server->unsubscribes);
    }

    /**
     * A commit answered with a plain PubAck (a server that predates fast ingest stored it as a plain publish) fails
     * as unsupported.
     */
    public function testACommitAnsweredWithAPlainPubAckIsUnsupported(): void
    {
        [$client] = $this->connect(static fn(array $m): array => ['{"stream":"NB","seq":1}']);
        $fast = $client->jetStream()->fastPublisher();

        $this->expectException(UnsupportedFeatureException::class);
        $this->expectExceptionMessage('stored the message as a plain publish (stream "NB", seq 1)');
        $fast->commit('fi.events', 'only')->await();
    }

    /**
     * U11 (case Q): an abandoned batch's 10208 surfaces at the next read: a stall here.
     */
    public function testAnUnknownBatchSurfacesAtTheNextStall(): void
    {
        [$client, $server] = $this->connect(static fn(array $m): array => match ($m['seq']) {
            1 => [self::ack(0, 1)],
            2 => ['{"error":{"code":400,"err_code":10208,"description":"batch publish ID unknown"},"stream":"FI","seq":0}'],
            default => [],
        });
        $fast = $client->jetStream()->fastPublisher(flow: 1, maxOutstandingAcks: 1);
        $fast->add('fi.events', 'm-1')->await();

        try {
            $fast->add('fi.events', 'm-2')->await();
            self::fail('expected the 10208 to surface');
        } catch (JetStreamException $e) {
            self::assertSame(10208, $e->getErrCode());
            self::assertSame(400, $e->getCode());
            self::assertSame('batch publish ID unknown', $e->getMessage());
        }
        self::assertTrue($fast->isClosed());
        self::assertSame([(string) $server->sid], $server->unsubscribes);
    }

    /**
     * U11 (case Q): the same at the commit.
     */
    public function testAnUnknownBatchSurfacesAtTheCommit(): void
    {
        [$client] = $this->connect(static fn(array $m): array => match ($m['seq']) {
            1 => [self::ack(0, 10)],
            3 => ['{"error":{"code":400,"err_code":10208,"description":"batch publish ID unknown"},"stream":"FI","seq":0}'],
            default => [],
        });
        $fast = $client->jetStream()->fastPublisher(flow: 10);
        $fast->add('fi.events', 'm-1')->await();
        $fast->add('fi.events', 'm-2')->await();

        try {
            $fast->commit('fi.events', 'm-3')->await();
            self::fail('expected the 10208 to surface');
        } catch (JetStreamException $e) {
            self::assertSame(10208, $e->getErrCode());
        }
        self::assertTrue($fast->isClosed());
    }

    /**
     * U12: the INFO version is never read: INFO says 2.10.29, the server answers as 2.14.10, and the batch is sent and
     * commits (ADR-44: the JetStream API may be answered by another release than the connected server's).
     */
    public function testTheInfoVersionIsNeverChecked(): void
    {
        $batchId = '';
        [$client, $server] = $this->connect(
            static function (array $m) use (&$batchId): array {
                return $m['op'] === 0 ? [self::ack(0, 10)] : ($m['op'] === 2 ? [self::pubAck(2, $batchId, 2)] : []);
            },
            'INFO {"server_id":"S1","server_name":"n1","version":"2.10.29","jetstream":true,"max_payload":1048576,"headers":true,"proto":1}',
        );
        $fast = $client->jetStream()->fastPublisher(flow: 10);
        $batchId = $fast->batchId();

        $fast->add('fi.events', 'm-1')->await();
        self::assertSame(2, $fast->commit('fi.events', 'm-2')->await()->batchCount);
        self::assertSame(['1.0', '2.2'], $server->sequenceAndOperations());
    }

    /**
     * @return iterable<string, array{array<string,mixed>, string}>
     */
    public static function invalidSettings(): iterable
    {
        yield 'flow 0' => [['flow' => 0], 'flow must be between 1 and 65535, 0 given'];
        yield 'flow 65536' => [['flow' => 65536], 'flow must be between 1 and 65535, 65536 given'];
        yield 'maxOutstandingAcks 0' => [['maxOutstandingAcks' => 0], 'maxOutstandingAcks must be between 1 and 3, 0 given'];
        yield 'maxOutstandingAcks 4' => [['maxOutstandingAcks' => 4], 'maxOutstandingAcks must be between 1 and 3, 4 given'];
        yield 'ackTimeoutMs 0' => [['ackTimeoutMs' => 0], 'ackTimeoutMs must be greater than zero, 0 given'];
        yield 'ackTimeoutMs -1' => [['ackTimeoutMs' => -1], 'ackTimeoutMs must be greater than zero, -1 given'];
    }

    /**
     * U13: a setting out of range is refused before anything is written.
     *
     * @param array<string,mixed> $settings
     */
    #[DataProvider('invalidSettings')]
    public function testASettingOutOfRangeIsRefused(array $settings, string $message): void
    {
        [$client, $server, $transport] = $this->connect(static fn(array $m): array => []);
        $writes = count($transport->writes);

        try {
            $client->jetStream()->fastPublisher(...$settings);
            self::fail('expected the setting to be refused');
        } catch (JetStreamException $e) {
            self::assertStringContainsString($message, $e->getMessage());
        }
        self::assertCount($writes, $transport->writes);
        self::assertNull($server->subscription);
    }

    /**
     * The edges of each range are accepted.
     */
    public function testTheEdgesOfEachRangeAreAccepted(): void
    {
        [$client] = $this->connect(static fn(array $m): array => []);
        $js = $client->jetStream();

        foreach ([['flow' => 1], ['flow' => 65535], ['maxOutstandingAcks' => 1], ['maxOutstandingAcks' => 3], ['ackTimeoutMs' => 1]] as $settings) {
            self::assertFalse($js->fastPublisher(...$settings)->isClosed());
        }
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function atomicBatchHeaders(): iterable
    {
        yield 'Nats-Batch-Id' => ['Nats-Batch-Id'];
        yield 'Nats-Batch-Sequence' => ['Nats-Batch-Sequence'];
        yield 'nats-batch-commit (any case)' => ['nats-batch-commit'];
    }

    /**
     * U13: a message carrying an atomic batch header is refused, by add() and by commit(), before anything is
     * written: fast ingest must not be combined with an atomic batch.
     */
    #[DataProvider('atomicBatchHeaders')]
    public function testAnAtomicBatchHeaderIsRefused(string $header): void
    {
        [$client, $server] = $this->connect(static fn(array $m): array => []);
        $fast = $client->jetStream()->fastPublisher();

        foreach ([fn() => $fast->add('fi.events', 'm', [$header => '1'])->await(), fn() => $fast->commit('fi.events', 'm', [$header => '1'])->await()] as $call) {
            try {
                $call();
                self::fail('expected the header to be refused');
            } catch (JetStreamException $e) {
                self::assertStringContainsString('must not carry the atomic batch header ' . $header, $e->getMessage());
            }
        }
        self::assertSame([], $server->messages);
        self::assertNull($server->subscription);
        self::assertFalse($fast->isClosed());
    }

    /**
     * U13: close() before any add() is refused, with nothing written.
     */
    public function testCloseBeforeAnyAddIsRefused(): void
    {
        [$client, $server] = $this->connect(static fn(array $m): array => []);
        $fast = $client->jetStream()->fastPublisher();

        try {
            $fast->close()->await();
            self::fail('expected close() to be refused');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('nothing to close', $e->getMessage());
        }
        self::assertSame([], $server->messages);
        self::assertNull($server->subscription);
    }

    /**
     * U14: a commit with no reply fails after the ack timeout, having pinged at the commit's sequence.
     */
    public function testACommitWithNoReplyFailsAfterTheAckTimeout(): void
    {
        [$client, $server] = $this->connect(static fn(array $m): array => $m['op'] === 0 ? [self::ack(0, 10)] : []);
        $fast = $client->jetStream()->fastPublisher(flow: 10, ackTimeoutMs: 300);
        $fast->add('fi.events', 'm-1')->await();

        $startNs = hrtime(true);
        try {
            $fast->commit('fi.events', 'm-2')->await();
            self::fail('expected a TimeoutException');
        } catch (TimeoutException $e) {
            self::assertStringContainsString('no PubAck for the commit at batch sequence 2 within 300 ms', $e->getMessage());
        }
        self::assertGreaterThanOrEqual(0.29, (hrtime(true) - $startNs) / 1e9);

        self::assertSame(['1.0', '2.2', '2.4', '2.4'], $server->sequenceAndOperations());
        self::assertTrue($fast->isClosed());
        self::assertSame([(string) $server->sid], $server->unsubscribes);
    }

    /**
     * No reply to the first message fails the first add() after the ack timeout, without a ping.
     */
    public function testNoReplyToTheFirstMessageFailsAfterTheAckTimeout(): void
    {
        [$client, $server] = $this->connect(static fn(array $m): array => []);
        $fast = $client->jetStream()->fastPublisher(ackTimeoutMs: 200);

        try {
            $fast->add('fi.events', 'm-1')->await();
            self::fail('expected a TimeoutException');
        } catch (TimeoutException $e) {
            self::assertStringContainsString('no reply to its first message within 200 ms', $e->getMessage());
        }
        self::assertSame(['1.0'], $server->sequenceAndOperations());
        self::assertTrue($fast->isClosed());
    }

    /**
     * U15: with `inboxPrefix: 'acc'` the SUB and every reply subject start with `acc.`.
     */
    public function testTheInboxFollowsTheInboxPrefix(): void
    {
        $batchId = '';
        [$client, $server] = $this->connect(static function (array $m) use (&$batchId): array {
            return $m['op'] === 0 ? [self::ack(0, 10)] : ($m['op'] === 2 ? [self::pubAck(2, $batchId, 2)] : []);
        }, inboxPrefix: 'acc');
        $fast = $client->jetStream()->fastPublisher(flow: 10);
        $batchId = $fast->batchId();

        $fast->add('fi.events', 'm-1')->await();
        $fast->commit('fi.events', 'm-2')->await();

        self::assertSame('acc.' . $batchId . '.>', $server->subscription);
        foreach ($server->messages as $message) {
            self::assertStringStartsWith('acc.' . $batchId . '.10.fail.', $message['reply']);
        }
    }

    /**
     * A second call while one is still running fails without sending anything: one fiber per publisher.
     */
    public function testAConcurrentCallIsRefused(): void
    {
        [$client, $server] = $this->connect(static fn(array $m): array => []);
        $fast = $client->jetStream()->fastPublisher(ackTimeoutMs: 500);

        $first = $fast->add('fi.events', 'm-1');
        $second = async(static fn() => $fast->add('fi.events', 'm-2')->await());
        try {
            $second->await();
            self::fail('expected the concurrent add() to be refused');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('not safe for concurrent use', $e->getMessage());
        }
        self::settle();
        self::assertSame(['1.0'], $server->sequenceAndOperations());

        $server->reply($server->replySubject(1, 0), self::ack(0, 10));
        self::assertSame(1, $first->await()->batchSequence);
    }

    /**
     * A control inbox the server rejects fails the first add() at once, the limit with a SubscriptionLimitException
     * and a permissions violation with a ConnectionException quoting it; the publisher is closed.
     *
     * @return iterable<string, array{string, class-string<\Throwable>, string}>
     */
    public static function inboxRejections(): iterable
    {
        yield 'the subscription limit' => [
            'maximum subscriptions exceeded',
            SubscriptionLimitException::class,
            'because the connection is at its subscription limit',
        ];
        yield 'a permissions violation' => [
            'Permissions Violation for Subscription to "{subject}"',
            ConnectionException::class,
            'Permissions Violation for Subscription to',
        ];
    }

    /**
     * @param class-string<\Throwable> $class
     */
    #[DataProvider('inboxRejections')]
    public function testARejectedControlInboxFailsTheFirstAddAtOnce(string $serverError, string $class, string $message): void
    {
        [$client, $server] = $this->connect(static fn(array $m): array => []);
        $server->rejectSubscriptionWith = $serverError;
        $fast = $client->jetStream()->fastPublisher(ackTimeoutMs: 1_500);

        $startNs = hrtime(true);
        try {
            $fast->add('fi.events', 'm-1')->await();
            self::fail('expected the rejected inbox to fail the add()');
        } catch (\Throwable $e) {
            self::assertSame($class, $e::class, $e->getMessage());
            self::assertStringContainsString('Fast batch publish failed', $e->getMessage());
            self::assertStringContainsString($message, $e->getMessage());
        }
        self::assertLessThan(1.0, (hrtime(true) - $startNs) / 1e9, 'it does not wait for the ack timeout');
        self::assertTrue($fast->isClosed());
    }

    /**
     * Control replies the client cannot read (not JSON, or a type it does not know) are notices: the batch goes on.
     * Without an onError the notices are dropped.
     */
    public function testUnreadableControlRepliesAreNotices(): void
    {
        $batchId = '';
        $respond = static function (array $m) use (&$batchId): array {
            return match ($m['seq']) {
                1 => [self::ack(0, 10)],
                2 => ['not json', '{"type":"later","seq":2}'],
                3 => [self::pubAck(3, $batchId, 3)],
                default => [],
            };
        };

        [$client] = $this->connect($respond);
        $notices = [];
        $fast = $client->jetStream()->fastPublisher(onError: static function (FastBatchException $notice) use (&$notices): void {
            $notices[] = $notice->getMessage();
        });
        $batchId = $fast->batchId();
        $fast->add('fi.events', 'm-1')->await();
        $fast->add('fi.events', 'm-2')->await();
        self::assertSame(3, $fast->commit('fi.events', 'm-3')->await()->batchCount);
        self::assertCount(2, $notices);
        self::assertStringContainsString('malformed control reply: not json', $notices[0]);
        self::assertStringContainsString('control reply of unknown type: {"type":"later","seq":2}', $notices[1]);

        $silent = $client->jetStream()->fastPublisher();
        $batchId = $silent->batchId();
        $silent->add('fi.events', 'm-1')->await();
        $silent->add('fi.events', 'm-2')->await();
        self::assertSame(3, $silent->commit('fi.events', 'm-3')->await()->batchCount);
    }

    /**
     * What the server sends after the reply that ended the batch (the 10208 answers to messages sent before the end
     * was read) changes nothing: the batch ended with its PubAck.
     */
    public function testRepliesAfterTheEndAreIgnored(): void
    {
        [$client, $server] = $this->connect(static fn(array $m): array => $m['seq'] === 1 ? [self::ack(0, 1)] : []);
        $fast = $client->jetStream()->fastPublisher(flow: 1, maxOutstandingAcks: 1);
        $fast->add('fi.events', 'm-1')->await();

        $second = $fast->add('fi.events', 'm-2');
        self::settle();
        $server->replyInOneChunk($server->replySubject(2, 1), [
            '{"type":"gap","last_seq":2,"seq":3}',
            self::pubAck(7, $fast->batchId(), 1),
            '{"error":{"code":400,"err_code":10208,"description":"batch publish ID unknown"},"stream":"FI","seq":0}',
        ]);

        try {
            $second->await();
            self::fail('expected the batch to have ended');
        } catch (FastBatchException $e) {
            self::assertSame(1, $e->pubAck?->batchCount);
            self::assertNull($e->getErrCode());
        }
    }

    /**
     * An end that another fiber's read delivered while no call ran fails the next call before it sends anything.
     */
    public function testAnEndReadByAnotherFiberFailsTheNextCallBeforeItSends(): void
    {
        [$client, $server] = $this->connect(static fn(array $m): array => $m['op'] === 0 ? [self::ack(0, 10)] : []);
        $fast = $client->jetStream()->fastPublisher(flow: 10);
        $fast->add('fi.events', 'm-1')->await();

        $server->reply($server->replySubject(1, 0), '{"error":{"code":400,"err_code":10208,"description":"batch publish ID unknown"},"stream":"FI","seq":0}');
        $client->processIncoming()->await();

        try {
            $fast->add('fi.events', 'm-2')->await();
            self::fail('expected the ended batch to refuse');
        } catch (JetStreamException $e) {
            self::assertSame(10208, $e->getErrCode());
        }
        self::assertSame(['1.0'], $server->sequenceAndOperations());
        self::assertTrue($fast->isClosed());
    }

    /**
     * The defaults are Orbit's: flow 100 and 2 outstanding acks; the ack timeout defaults to the request timeout.
     */
    public function testTheDefaults(): void
    {
        self::assertSame(100, FastPublisher::DEFAULT_FLOW);
        self::assertSame(2, FastPublisher::DEFAULT_MAX_OUTSTANDING_ACKS);
        self::assertSame(65535, FastPublisher::MAX_FLOW);
        self::assertSame(3, FastPublisher::MAX_OUTSTANDING_ACKS);

        [$client, $server] = $this->connect(static fn(array $m): array => []);
        $fast = $client->jetStream()->fastPublisher();
        $startNs = hrtime(true);
        try {
            $fast->add('fi.events', 'm-1')->await();
            self::fail('expected a TimeoutException');
        } catch (TimeoutException $e) {
            self::assertStringContainsString('within 2000 ms', $e->getMessage());
        }
        self::assertGreaterThanOrEqual(1.99, (hrtime(true) - $startNs) / 1e9);
        self::assertSame(100, $server->messages[0]['flow']);
        self::assertSame('fail', $server->messages[0]['gap']);
    }
}
