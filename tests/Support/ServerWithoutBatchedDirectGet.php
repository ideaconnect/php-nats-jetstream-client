<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

/**
 * Makes a scripted server answer every batched Direct Get as a server without batched Direct Get does (before 2.11;
 * 2.10.29 seen live, #272): it does not read `multi_last`, finds the request empty and answers `408 Empty Request` on
 * the request's inbox. KV getAll() and Object Store list() then take their per-subject Direct Get fan-out, which the
 * test scripts as before. Since #272 they try the batched request on every server instead of reading the INFO
 * version, so a test of the fan-out installs this after its own write responder.
 */
final class ServerWithoutBatchedDirectGet
{
    /**
     * Wraps $transport's write responder: each SUB of a Direct Get batch inbox (`_INBOX.JS.DGET.*`) is answered with
     * the 408 on that subscription's sid, after whatever the responder answers.
     */
    public static function install(FakeTransport $transport): void
    {
        $inner = $transport->onWrite;
        $transport->onWrite = static function (string $bytes) use ($inner): array {
            $frames = $inner === null ? [] : $inner($bytes);
            if (preg_match('/^SUB (_INBOX\.JS\.DGET\.\S+) (\d+)\r\n/', $bytes, $match) === 1) {
                $status = "NATS/1.0 408 Empty Request\r\n\r\n";
                $frames[] = sprintf("HMSG %s %s %d %d\r\n%s\r\n", $match[1], $match[2], strlen($status), strlen($status), $status);
            }

            return $frames;
        };
    }
}
