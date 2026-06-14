<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Services\ServiceError;

/**
 * Kills the surviving MethodCallRemoval mutant on src/Services/ServiceError.php:31, where
 * `parent::__construct($description)` is dropped. Without that call the underlying
 * \RuntimeException message stays the default empty string, so getMessage() would no longer
 * echo the description supplied to the ServiceError constructor.
 */
final class ServiceErrorMutationTest extends \PHPUnit\Framework\TestCase
{
    public function testParentConstructorPropagatesDescriptionAsExceptionMessage(): void
    {
        $error = new ServiceError(429, 'Rate limited', '{"retry_after":5}');

        // kills MethodCallRemoval @ line 31: dropping parent::__construct($description) leaves
        // the RuntimeException message empty (''), but the real code must surface 'Rate limited'.
        self::assertSame('Rate limited', $error->getMessage());

        // The dedicated readonly property is set independently of the parent call, so pin it too
        // to prove the assertion above is specifically about the parent constructor propagation.
        self::assertSame('Rate limited', $error->description);
        self::assertSame('429', $error->serviceErrorCode);
        self::assertSame('{"retry_after":5}', $error->body);
    }

    public function testMessageIsNonEmptyWhenThrownAndCaught(): void
    {
        // A second angle on the same mutant: catching the thrown error must observe the
        // description on the Throwable message channel, which only happens via parent::__construct.
        $caught = null;

        try {
            throw new ServiceError('422', 'Unprocessable', null);
        } catch (ServiceError $thrown) {
            $caught = $thrown;
        }

        // Proves the catch branch actually ran (assertInstanceOf, unlike assertNotNull, is not
        // statically pre-narrowed away here) and pins the exact thrown type.
        self::assertInstanceOf(ServiceError::class, $caught);

        // kills MethodCallRemoval @ line 31: empty message would make this fail.
        self::assertNotSame('', $caught->getMessage());
        self::assertSame('Unprocessable', $caught->getMessage());
        self::assertNull($caught->body);
    }
}
