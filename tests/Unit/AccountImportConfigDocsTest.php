<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use PHPUnit\Framework\TestCase;

/**
 * Guards #284: the README's "Account imports" config block (the first fenced block of `README.md`
 * that starts with `accounts {`) must stay equal to the `accounts { }` block the `nats-accounts`
 * fixture actually runs (`build/nats/accounts.conf`), so the two can only drift apart on purpose. The
 * fixture adds one `users:` line to each account, which the README does not need to show; those
 * lines are taken out before the comparison.
 */
final class AccountImportConfigDocsTest extends TestCase
{
    public function testReadmeAccountsBlockMatchesFixtureConfigOnceUsersAreTakenOut(): void
    {
        $readmeBlock = $this->readmeAccountsBlock();
        [$fixtureBlock, $userLines] = $this->fixtureAccountsBlockWithoutUserLines();

        self::assertGreaterThan(
            0,
            $userLines,
            'build/nats/accounts.conf names no user: the fixture would be unreachable.',
        );

        self::assertSame(
            $readmeBlock,
            $fixtureBlock,
            'README.md\'s "Account imports" accounts { } block has drifted from build/nats/accounts.conf.',
        );
    }

    /**
     * Returns the first fenced block of README.md whose content starts with `accounts {`.
     */
    private function readmeAccountsBlock(): string
    {
        $path = dirname(__DIR__, 2) . '/README.md';
        $readme = file_get_contents($path);
        self::assertNotFalse($readme, 'README.md could not be read.');

        if (preg_match('/```\n(accounts \{.*?\n\})\n```/s', $readme, $matches) !== 1) {
            self::fail('README.md has no fenced ```\naccounts { ... }\n``` block.');
        }

        return $matches[1];
    }

    /**
     * Returns the `accounts { }` block of build/nats/accounts.conf (from its first `accounts {` to
     * the end of the file) with every line that starts with `users:` removed, and how many such
     * lines were removed.
     *
     * @return array{string, int}
     */
    private function fixtureAccountsBlockWithoutUserLines(): array
    {
        $path = dirname(__DIR__, 2) . '/build/nats/accounts.conf';
        $config = file_get_contents($path);
        self::assertNotFalse($config, 'build/nats/accounts.conf could not be read.');

        $start = strpos($config, "accounts {\n");
        if ($start === false) {
            self::fail('build/nats/accounts.conf has no accounts { } block.');
        }

        $block = rtrim(substr($config, $start));

        $kept = [];
        $userLines = 0;
        foreach (explode("\n", $block) as $line) {
            if (str_starts_with(ltrim($line), 'users:')) {
                $userLines++;
                continue;
            }

            $kept[] = $line;
        }

        return [implode("\n", $kept), $userLines];
    }
}
