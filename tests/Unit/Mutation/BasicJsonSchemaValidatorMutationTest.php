<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Services\BasicJsonSchemaValidator;
use PHPUnit\Framework\TestCase;

/**
 * Mutation-killing tests for {@see BasicJsonSchemaValidator}.
 *
 * Each method pins the exact observable behavior that an Infection-discovered
 * surviving mutant would change. See `// kills ...` comments per assertion group.
 */
final class BasicJsonSchemaValidatorMutationTest extends TestCase
{
    /**
     * @param array<string, mixed> $schema
     */
    private function validate(string $payload, array $schema): ?string
    {
        $validator = new BasicJsonSchemaValidator();
        $message = new NatsMessage('svc.echo', 1, '_INBOX.1', $payload, null);

        return $validator->validate($message, $schema);
    }

    /**
     * Line 41: the object-branch guard is `(type === 'object') && is_array($value)`.
     *
     * Identical mutant turns it into `type ?? (null !== 'object')` (precedence makes
     * `!==` bind tighter than `??`), i.e. effectively `type` (truthy) for any non-null
     * type, so the required-property check runs even for a non-object schema type.
     *
     * LogicalAnd mutant turns `&&` into `||`, so with a non-object type but an array
     * value the branch is entered as well.
     *
     * For a schema of type 'array' over a list value with a 'required' entry, the REAL
     * code skips the required check entirely (type is not 'object') and returns null.
     * Both mutants would instead enter the branch and report the missing required key.
     */
    public function testNonObjectTypeDoesNotRunRequiredCheck(): void
    {
        // kills Identical @ 41, kills LogicalAnd @ 41
        $error = $this->validate('[1,2,3]', [
            'type' => 'array',
            'required' => ['id'],
        ]);

        self::assertNull($error);
    }

    /**
     * Line 55: malformed property entries are skipped with `continue`, not `break`.
     *
     * The properties map is iterated in insertion order. The first entry is malformed
     * (non-array schema) and must be skipped so the loop reaches the second, valid
     * property whose payload value has the wrong type. With `continue` the validator
     * reports that later type error; with `break` it stops at the malformed entry and
     * returns null.
     */
    public function testMalformedPropertySchemaIsSkippedNotBreaking(): void
    {
        // kills Continue_ @ 55
        $error = $this->validate('{"id":7,"name":123}', [
            'type' => 'object',
            'properties' => [
                'id' => 'not-an-array-schema', // malformed -> continue (skip)
                'name' => ['type' => 'string'], // valid, value 123 is wrong type
            ],
        ]);

        self::assertSame('$.name must be string, got int', $error);
    }

    /**
     * Line 59: a property declared in the schema but absent from the object is skipped
     * with `continue`, not `break`.
     *
     * The first declared property ('missing') is not present in the payload and must be
     * skipped so the loop reaches the second declared property ('name'), which is present
     * with a wrong-typed value. With `continue` the later type error is reported; with
     * `break` the loop stops at the absent property and returns null.
     */
    public function testAbsentPropertyIsSkippedNotBreaking(): void
    {
        // kills Continue_ @ 59
        $error = $this->validate('{"name":123}', [
            'type' => 'object',
            'properties' => [
                'missing' => ['type' => 'string'], // absent in payload -> continue (skip)
                'name' => ['type' => 'string'], // present, value 123 is wrong type
            ],
        ]);

        self::assertSame('$.name must be string, got int', $error);
    }

    /**
     * Line 77: the 'string' match arm. Removing it makes 'string' fall to `default => true`,
     * so a non-string value would be (wrongly) accepted. Real code rejects integer 5.
     */
    public function testStringArmRejectsNonString(): void
    {
        // kills MatchArmRemoval ('string') @ 77
        self::assertSame('$ must be string, got int', $this->validate('5', ['type' => 'string']));
    }

    /**
     * Line 77: the 'boolean' match arm. Removing it makes 'boolean' fall to `default => true`.
     * Real code rejects integer 1 as a non-boolean.
     */
    public function testBooleanArmRejectsNonBoolean(): void
    {
        // kills MatchArmRemoval ('boolean') @ 77
        self::assertSame('$ must be boolean, got int', $this->validate('1', ['type' => 'boolean']));
    }

    /**
     * Line 77: the 'null' match arm. Removing it makes 'null' fall to `default => true`.
     * Real code rejects integer 1 as non-null.
     */
    public function testNullArmRejectsNonNull(): void
    {
        // kills MatchArmRemoval ('null') @ 77
        self::assertSame('$ must be null, got int', $this->validate('1', ['type' => 'null']));
    }

    /**
     * Line 77/80: the 'number' match arm and its `is_int || is_float` test.
     *
     * - MatchArmRemoval ('number'): removing the arm falls to `default => true`, accepting
     *   a non-number. Real code rejects the string "x".
     * - LogicalOrAllSubExprNegation: `is_int($v) || is_float($v)` -> `!is_int($v) || !is_float($v)`.
     *   For the string "x" the real expr is `false||false` = invalid (error); the mutant is
     *   `true||true` = valid (null). Asserting the error kills it.
     */
    public function testNumberArmRejectsNonNumber(): void
    {
        // kills MatchArmRemoval ('number') @ 77, kills LogicalOrAllSubExprNegation @ 80
        self::assertSame('$ must be number, got string', $this->validate('"x"', ['type' => 'number']));
    }

    /**
     * Line 80: `is_int($v) || is_float($v)` for the 'number' arm.
     *
     * LogicalOrSingleSubExprNegation turns it into `!is_int($v) || is_float($v)`. For an
     * integer value the real expr is `true||false` = valid (null); the mutant is
     * `false||false` = invalid (error). Real code accepts integer 7.
     */
    public function testNumberArmAcceptsInteger(): void
    {
        // kills LogicalOrSingleSubExprNegation @ 80
        self::assertNull($this->validate('7', ['type' => 'number']));
    }

    /**
     * Line 82: `is_array($v) && array_is_list($v)` for the 'array' arm.
     *
     * - LogicalAndAllSubExprNegation: `!is_array && !array_is_list`. For a JSON list the real
     *   expr is `true && true` = valid (null); the mutant is `false && false` = invalid (error).
     * - LogicalAndSingleSubExprNegation: `!is_array && array_is_list`. For a JSON list the
     *   mutant is `false && true` = invalid (error), while real is valid (null).
     *
     * Asserting that a JSON list is accepted as type 'array' kills both.
     */
    public function testArrayArmAcceptsJsonList(): void
    {
        // kills LogicalAndAllSubExprNegation @ 82, kills LogicalAndSingleSubExprNegation @ 82
        self::assertNull($this->validate('[1,2,3]', ['type' => 'array']));
    }
}
