<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\JetStream\Models\ConsumerInfo;
use PHPUnit\Framework\TestCase;

/**
 * Mutation-killing unit tests for {@see ConsumerInfo::fromArray()}.
 *
 * The factory hydrates an immutable value object from a JetStream CONSUMER.INFO payload:
 *   - line 37: `$deliverSubject = (string) ($config['deliver_subject'] ?? '')` — the cast forces a
 *     string, so a non-string falsy value (e.g. bool false) collapses to '' and yields push=false.
 *   - line 41: `name: (string) ($data['name'] ?? ($config['durable_name'] ?? ''))` — top-level
 *     `name` wins, falling back to the nested `config.durable_name`, finally to ''.
 *
 * Source must NOT be modified. Tests are pure construction + value assertions: deterministic, no I/O.
 */
final class ConsumerInfoMutationTest extends TestCase
{
    /**
     * A non-string falsy `deliver_subject` (bool false) must be cast to '' first, so
     * `$deliverSubject !== ''` is false and the consumer is pull-based (push=false).
     *
     * The CastString mutant drops the `(string)` cast, leaving the raw `false`; then
     * `false !== ''` is a strict comparison between distinct types and evaluates to TRUE,
     * flipping push to true. This input is exactly where cast vs no-cast diverge.
     */
    public function testNonStringFalsyDeliverSubjectIsCastBeforeEmptinessCheck(): void
    {
        // kills CastString @ line 37
        $info = ConsumerInfo::fromArray([
            'config' => ['deliver_subject' => false],
        ]);

        self::assertFalse($info->push);
    }

    /**
     * A real, non-empty `deliver_subject` string marks the consumer push-based. Belt-and-braces
     * for the cast/`!== ''` boundary so the false-case above is not the only push assertion.
     */
    public function testPresentDeliverSubjectMakesConsumerPush(): void
    {
        $info = ConsumerInfo::fromArray([
            'config' => ['deliver_subject' => '_INBOX.deliver'],
        ]);

        self::assertTrue($info->push);
    }

    /**
     * When the top-level `name` is absent, the name must fall back to the nested
     * `config.durable_name`.
     *
     * The Coalesce mutant `('' ?? $config['durable_name'])` rewrites the fallback to a literal '',
     * which `??` never skips, so the durable name would be dropped and '' returned instead.
     */
    public function testNameFallsBackToDurableNameWhenTopLevelNameMissing(): void
    {
        // kills Coalesce @ line 41 (name ?? ('' ?? durable_name) variant)
        $info = ConsumerInfo::fromArray([
            'config' => ['durable_name' => 'my-durable'],
        ]);

        self::assertSame('my-durable', $info->name);
    }

    /**
     * When BOTH the top-level `name` and `config.durable_name` are present and differ, the
     * top-level `name` wins.
     *
     * The Coalesce mutant `$config['durable_name'] ?? $data['name'] ?? ''` reverses precedence,
     * returning the durable name instead. This input distinguishes the two orderings.
     */
    public function testTopLevelNameTakesPrecedenceOverDurableName(): void
    {
        // kills Coalesce @ line 41 (durable_name ?? name variant)
        $info = ConsumerInfo::fromArray([
            'name' => 'ephemeral-xyz',
            'config' => ['durable_name' => 'my-durable'],
        ]);

        self::assertSame('ephemeral-xyz', $info->name);
    }
}
