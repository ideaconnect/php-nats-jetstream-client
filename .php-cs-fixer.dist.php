<?php

declare(strict_types=1);

use PhpCsFixer\Config;
use PhpCsFixer\Finder;

$finder = (new Finder())
    ->in([
        __DIR__ . '/src',
        __DIR__ . '/tests',
    ])
    ->name('*.php');

return (new Config())
    ->setRiskyAllowed(true)
    ->setRules([
        // Modern baseline: PER Coding Style 3.0 (the PSR-12 successor) plus PHP 8.2 modernization.
        '@PER-CS3.0' => true,
        '@PER-CS3.0:risky' => true,
        '@PHP82Migration' => true,
        '@PHP82Migration:risky' => true,

        // Project conventions and a few widely-used quality rules.
        'declare_strict_types' => true,
        'array_syntax' => ['syntax' => 'short'],
        'single_quote' => true,
        'no_unused_imports' => true,
        'ordered_imports' => [
            'sort_algorithm' => 'alpha',
            'imports_order' => ['class', 'function', 'const'],
        ],
        'trailing_comma_in_multiline' => [
            'elements' => ['arrays', 'arguments', 'parameters', 'match'],
        ],
        // Keep the extensive @param/@return docblocks the codebase relies on.
        'no_superfluous_phpdoc_tags' => false,
    ])
    ->setFinder($finder);
