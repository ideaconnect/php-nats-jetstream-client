<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Services\BasicJsonSchemaValidator;
use PHPUnit\Framework\TestCase;

final class BasicJsonSchemaValidatorTest extends TestCase
{
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
}
