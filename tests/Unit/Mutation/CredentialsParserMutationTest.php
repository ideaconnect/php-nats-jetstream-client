<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Auth\CredentialsParser;
use IDCT\NATS\Exception\NatsException;
use PHPUnit\Framework\TestCase;

/**
 * Mutation-killing tests for {@see CredentialsParser}.
 *
 * Each method pins the exact observable behavior that an Infection mutant would change.
 */
final class CredentialsParserMutationTest extends TestCase
{
    /**
     * A directory is the discriminating input for `||` vs `&&` on line 21:
     * for a directory `is_file()` is false but `is_readable()` is true, so
     * `!is_file($path) || !is_readable($path)` is true (the guard fires and throws),
     * whereas the mutated `&&` would be false (guard skipped, falls through to
     * file_get_contents on a directory and ultimately a *different* parse failure).
     */
    public function testFromFileRejectsDirectoryPathBecauseGuardIsOr(): void
    {
        $dir = sys_get_temp_dir();
        self::assertDirectoryExists($dir);
        self::assertFalse(is_file($dir));
        self::assertTrue(is_readable($dir));

        try {
            CredentialsParser::fromFile($dir);
            self::fail('Expected NatsException for a directory path.');
        } catch (NatsException $e) {
            // kills LogicalOr @ line 21 — must be the readability guard message,
            // NOT a downstream "does not contain ... block" parse error.
            self::assertStringContainsString('not found or not readable', $e->getMessage());
        }
    }

    /**
     * The not-found/not-readable message is built as
     * `'Credentials file not found or not readable: ' . $path`.
     */
    public function testNotReadableMessageOrderingAndIncludesPath(): void
    {
        $path = '/nonexistent/path/does-not-exist-12345.creds';

        try {
            CredentialsParser::fromFile($path);
            self::fail('Expected NatsException for a missing path.');
        } catch (NatsException $e) {
            $message = $e->getMessage();

            // kills Concat @ line 22 — operands must be in order:
            // the literal prefix comes first, the path is appended last.
            self::assertStringStartsWith('Credentials file not found or not readable: ', $message);

            // kills ConcatOperandRemoval @ line 22 — the path must be present in the message.
            self::assertStringContainsString($path, $message);
            self::assertStringEndsWith($path, $message);

            // Exact full message nails both mutants unambiguously.
            self::assertSame('Credentials file not found or not readable: ' . $path, $message);
        }
    }

    /**
     * The extracted block content is passed through trim(), so surrounding
     * whitespace inside the captured group must be stripped.
     */
    public function testExtractedBlocksAreTrimmed(): void
    {
        // Leading and trailing spaces around each value live INSIDE the captured group.
        $contents = "-----BEGIN NATS USER JWT-----\n  jwt-value-x  \n-----END NATS USER JWT-----\n"
            . "\n-----BEGIN USER NKEY SEED-----\n\tseed-value-y\t\n-----END USER NKEY SEED-----\n";

        $result = CredentialsParser::parse($contents);

        // kills UnwrapTrim @ line 70 — without trim() these would retain the
        // surrounding whitespace ("  jwt-value-x  " / "\tseed-value-y\t").
        self::assertSame('jwt-value-x', $result['jwt']);
        self::assertSame('seed-value-y', $result['nkeySeed']);
    }
}
