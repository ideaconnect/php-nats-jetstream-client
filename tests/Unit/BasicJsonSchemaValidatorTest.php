<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Services\BasicJsonSchemaValidator;
use PHPUnit\Framework\TestCase;

final class BasicJsonSchemaValidatorTest extends TestCase
{
    /**
     * A JSON LIST must fail `"type":"object"`: PHP's assoc decoding makes both maps and lists
     * arrays, and the old bare is_array() check accepted `[1,2,3]` where every JSON Schema
     * validator rejects it. The empty case stays accepted - `{}` and `[]` are indistinguishable
     * after assoc decoding.
     */
    public function testObjectTypeRejectsNonEmptyJsonList(): void
    {
        $validator = new BasicJsonSchemaValidator();
        $schema = ['type' => 'object', 'properties' => ['id' => ['type' => 'integer']]];
        $message = static fn (string $payload): NatsMessage => new NatsMessage('svc.echo', 1, '_INBOX.1', $payload, null);

        self::assertNotNull($validator->validate($message('[1,2,3]'), $schema), 'a non-empty list is not an object');
        self::assertNull($validator->validate($message('{"id":5}'), $schema), 'a map still validates');
        self::assertNull($validator->validate($message('{}'), $schema), 'the empty object stays accepted ({} decodes to [])');
    }

    public function testRejectsInvalidJsonPayload(): void
    {
        $validator = new BasicJsonSchemaValidator();
        $message = new NatsMessage('svc.echo', 1, '_INBOX.1', '{invalid', null);

        $error = $validator->validate($message, ['type' => 'object']);

        self::assertSame('payload is not valid JSON', $error);
    }

    public function testRejectsMissingRequiredField(): void
    {
        $validator = new BasicJsonSchemaValidator();
        $message = new NatsMessage('svc.echo', 1, '_INBOX.1', '{"name":"john"}', null);

        $error = $validator->validate($message, [
            'type' => 'object',
            'required' => ['id'],
            'properties' => [
                'id' => ['type' => 'integer'],
                'name' => ['type' => 'string'],
            ],
        ]);

        self::assertSame('$.id is required', $error);
    }

    public function testRejectsWrongPropertyType(): void
    {
        $validator = new BasicJsonSchemaValidator();
        $message = new NatsMessage('svc.echo', 1, '_INBOX.1', '{"id":"abc"}', null);

        $error = $validator->validate($message, [
            'type' => 'object',
            'required' => ['id'],
            'properties' => [
                'id' => ['type' => 'integer'],
            ],
        ]);

        self::assertSame('$.id must be integer, got string', $error);
    }

    public function testAcceptsValidPayload(): void
    {
        $validator = new BasicJsonSchemaValidator();
        $message = new NatsMessage('svc.echo', 1, '_INBOX.1', '{"id":7,"name":"john"}', null);

        $error = $validator->validate($message, [
            'type' => 'object',
            'required' => ['id', 'name'],
            'properties' => [
                'id' => ['type' => 'integer'],
                'name' => ['type' => 'string'],
            ],
        ]);

        self::assertNull($error);
    }

    public function testValidatesAdditionalPrimitiveTypes(): void
    {
        $validator = new BasicJsonSchemaValidator();

        $boolMessage = new NatsMessage('svc.echo', 1, '_INBOX.1', 'true', null);
        self::assertNull($validator->validate($boolMessage, ['type' => 'boolean']));

        $numberMessage = new NatsMessage('svc.echo', 1, '_INBOX.1', '3.14', null);
        self::assertNull($validator->validate($numberMessage, ['type' => 'number']));

        $nullMessage = new NatsMessage('svc.echo', 1, '_INBOX.1', 'null', null);
        self::assertNull($validator->validate($nullMessage, ['type' => 'null']));

        $arrayMessage = new NatsMessage('svc.echo', 1, '_INBOX.1', '{"id":1}', null);
        self::assertSame('$ must be array, got array', $validator->validate($arrayMessage, ['type' => 'array']));
    }

    public function testUnknownTypeIsIgnored(): void
    {
        $validator = new BasicJsonSchemaValidator();
        $message = new NatsMessage('svc.echo', 1, '_INBOX.1', '{"id":7}', null);

        $error = $validator->validate($message, ['type' => 'custom-type']);

        self::assertNull($error);
    }

    public function testRejectsObjectTypeWhenPayloadIsNotObject(): void
    {
        $validator = new BasicJsonSchemaValidator();
        $message = new NatsMessage('svc.echo', 1, '_INBOX.1', '"plain-string"', null);

        $error = $validator->validate($message, ['type' => 'object']);

        self::assertSame('$ must be object, got string', $error);
    }

    public function testIgnoresMalformedRequiredAndPropertiesSchemaNodes(): void
    {
        $validator = new BasicJsonSchemaValidator();
        $message = new NatsMessage('svc.echo', 1, '_INBOX.1', '{"id":7}', null);

        $error = $validator->validate($message, [
            'type' => 'object',
            'required' => 'id',
            'properties' => [
                'id' => 'integer',
                1 => ['type' => 'string'],
            ],
        ]);

        self::assertNull($error);
    }

    /**
     * Verifies that a property defined in the schema but absent from the object is skipped.
     * An optional property that is not present in the payload should not trigger a type error.
     */
    public function testSkipsPropertyNotPresentInObject(): void
    {
        $validator = new BasicJsonSchemaValidator();
        // Payload has only 'id'; 'name' is in properties but not required and not in the object.
        $message = new NatsMessage('svc.echo', 1, '_INBOX.1', '{"id":7}', null);

        $error = $validator->validate($message, [
            'type' => 'object',
            'required' => ['id'],
            'properties' => [
                'id' => ['type' => 'integer'],
                'name' => ['type' => 'string'],
            ],
        ]);

        self::assertNull($error);
    }
}
