<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\JetStream\JetStreamContext;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\PullServerTrait;
use PHPUnit\Framework\TestCase;

/**
 * Kills surviving Infection mutants in JetStreamContext::consumePipelined() (chunk 3/4, #120 pipelined
 * pull engine). Only the token-sequence mutant at line 2405 produces a deterministic, wall-clock-free
 * observable through this in-process FakeTransport harness; the remaining survivors in this chunk are
 * timing constants / liveness optimizations / equivalent mutations (documented in the agent report).
 */
final class JetStreamContext_10MutationTest extends TestCase
{
    use PullServerTrait;

    /**
     * The reply-to token (final dot-segment of "_INBOX.JS.PULL.<nuid>.<token>") of every
     * CONSUMER.MSG.NEXT pull PUB written to the transport, in issue order. The token is the string the
     * engine derives from `dechex($tokenSeq)` and publishes as the pull's reply subject.
     *
     * @return list<string>
     */
    private function pullReplyTokens(FakeTransport $transport): array
    {
        $tokens = [];
        foreach ($transport->writes as $write) {
            if (!str_starts_with($write, 'PUB $JS.API.CONSUMER.MSG.NEXT.')) {
                continue;
            }

            $head = (string) strtok($write, "\r\n");
            $replyTo = explode(' ', $head)[2] ?? '';
            $segments = explode('.', $replyTo);
            $tokens[] = (string) end($segments);
        }

        return $tokens;
    }

    /**
     * kills Increment @ line 2405 (`dechex($tokenSeq++)` -> `dechex($tokenSeq--)`).
     *
     * The engine mints each pull's reply-subject token from a monotonically INCREASING counter, so two
     * serial pulls carry reply tokens "0" then "1". The post-decrement mutant would emit "0" then
     * dechex(-1) = "ffffffffffffffff" (a 64-bit wraparound). Routing still works either way (the fake
     * server echoes onto whatever reply-to it was handed and the router re-keys by that same token), so
     * the delivered payloads/count are identical - the ONLY divergence is the literal token bytes on the
     * wire, which we pin here.
     */
    public function testSecondPullReplyTokenIncrementsRatherThanDecrements(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'S', 'C', [
            [['msg' => 'a']],
            [['msg' => 'b']],
        ]);
        $js = $this->context($transport);

        $received = [];
        $processed = $js->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setIterations(2)
            ->handle(function (NatsMessage $message) use (&$received): void {
                $received[] = $message->payload;
            })->await();

        // Behaviour is identical under the mutant; these guard that the run actually issued two pulls.
        self::assertSame(2, $processed);
        self::assertSame(['a', 'b'], $received);

        $tokens = $this->pullReplyTokens($transport);
        self::assertCount(2, $tokens, 'finite N=2 must issue exactly two serial pulls');

        // First token is "0" under both increment and decrement (post-op returns 0 before stepping).
        self::assertSame('0', $tokens[0]);
        // The kill: real code steps the counter UP to 1; the mutant steps it DOWN to -1.
        self::assertSame('1', $tokens[1]);
        self::assertNotSame('ffffffffffffffff', $tokens[1]);
    }
}
