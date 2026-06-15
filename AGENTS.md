# AGENTS.md

This repository is an async-first PHP NATS and JetStream client built on Amp.

Use this file as the authoritative project guide for coding agents working in this repository. If another instruction file is shorter or less specific, follow this file.

## Repository Purpose

- Library: async NATS client with JetStream, KeyValue, ObjectStore, and service-framework support.
- Runtime stack: PHP 8.2+, Amp 3.x, Amp Socket, PHPUnit 11, PHPStan 2, PHP CS Fixer.
- Test infrastructure: local Docker Compose NATS fixtures, including token, user/pass, TLS, NKey, and JWT-auth servers.

## Repository Structure

- `src/Auth/`
  Authentication helpers such as credentials parsing, nonce signing, and NKey seed handling.

- `src/Connection/`
  Connection runtime, options, reconnect logic, and connection-state enums.

- `src/Core/`
  High-level client primitives: `NatsClient`, messages, headers, inbox generation, polling queue API.

- `src/Exception/`
  Library exception hierarchy. Prefer existing exception types over introducing new ones.

- `src/JetStream/`
  JetStream context, API subject helpers, configuration builders, consumers, KeyValue, ObjectStore, and response models.

- `src/Protocol/`
  Wire codec, parser, parsed frame model, and server INFO handling.

- `src/Services/`
  Microservice framework, endpoint registration, schema validation, and service grouping.

- `src/Transport/`
  Transport abstraction and Amp socket implementation.

- `tests/Unit/`
  Fast unit tests. Update these whenever behavior or validation changes.

- `tests/Integration/`
  Live NATS integration tests. These are gated by `RUN_INTEGRATION=1` and target the Docker Compose fixtures.

- `tests/Behat/`
  Behat step definitions and support helpers for end-to-end feature coverage against the real fixture stack.

- `tests/Support/`
  Fakes and helpers shared by tests.

- `features/`
  Behat Gherkin feature files grouped by domain (`core`, `auth`, `jetstream-core`, `jetstream-data`, `services`, `resilience`).

- `scripts/`
  Repo automation: coverage checks, JWT fixture generation/checking, NATS readiness, repeated integration runs, and full end-to-end test flow.

- `build/nats/`
  NATS config fixtures for token, user/pass, TLS, NKey, and JWT services.

- `build/nats/jwt/`
  Generated JWT/NKey artifacts used by the JWT-auth NATS fixture.
  Do not hand-edit these files.

- `build/tls/`
  TLS fixture material used by integration tests.

- `docker-compose.yml`
  Local integration fixture stack. Services expose client ports `14222` to `14227` and monitoring ports `18222` to `18227`.

## Commands Agents Should Know

### Core development commands

- Install dependencies:
  `composer install`

- Run all tests known to PHPUnit:
  `composer test`

- Run unit tests only:
  `composer test:unit`

- Run integration tests only:
  `RUN_INTEGRATION=1 composer test:integration`

- Run Behat feature tests:
  `composer test:bdd`

- Run a specific Behat suite:
  `BEHAT_SUITE=core composer test:bdd`

- Run repeated integration tests for flake detection:
  `composer test:integration:repeat`

- Run full end-to-end validation:
  `composer test:e2e`

- Run static analysis:
  `composer stan`

- Apply coding style fixes:
  `composer fix`

- Generate coverage summary:
  `composer coverage`

- Enforce coverage threshold:
  `composer coverage:check`

### JWT fixture commands

- Regenerate JWT fixtures intentionally:
  `composer fixture:jwt`

- Check whether committed JWT fixtures drift from generated output:
  `composer fixture:jwt:check`

## Testing Workflow

Choose the narrowest useful test first, then broaden only as needed.

### Minimum expectations by change type

- Pure doc or instruction-file changes:
  No PHP tests required.

- Small implementation change in one class:
  Run the relevant unit test file, then `composer test:unit` if behavior changed meaningfully.

- Changes in `src/Protocol/`, `src/Connection/`, `src/Auth/`, or `src/Transport/`:
  Run targeted unit tests, then `composer test:unit`.
  If auth or wire behavior changed, prefer `composer test:e2e` before finishing.

- Changes in `src/JetStream/`, `src/Services/`, or integration scripts:
  Run targeted unit tests if present, then `composer test:e2e`.

- Changes that alter a documented workflow or README example:
  Run the relevant Behat suite if one exists, then broaden to `composer test:bdd` or `composer test:e2e` when the change affects multiple flows.

- Changes to `docker-compose.yml`, `build/nats/*.conf`, JWT fixtures, or integration bootstrap:
  Run `composer fixture:jwt:check` if JWT-related, then `composer test:e2e`.

### Integration test facts

- Integration tests skip unless `RUN_INTEGRATION=1`.
- Default integration endpoints come from `tests/Integration/IntegrationTestBootstrap.php` and target local fixture ports.
- The preferred local flow is `composer test:e2e`.
- Behat feature tests reuse the same Docker Compose fixture stack and readiness flow via `composer test:bdd`.
- `scripts/run-tests-e2e.sh` performs JWT fixture validation first, then starts compose, waits for readiness, runs unit tests, runs integration tests, and runs Behat.
- `scripts/run-tests-bdd.sh` performs the same fixture preflight and readiness flow before running Behat.
- `KEEP_NATS_SERVICES=1 composer test:e2e` leaves compose services running after the run.
- `SKIP_JWT_FIXTURE_CHECK=1 composer test:e2e` skips JWT fixture validation when explicitly desired.
- `BEHAT_SUITE=core composer test:e2e` narrows only the Behat stage while keeping the unit/integration steps intact.

## Development Standards

### PHP style and language usage

- Keep `declare(strict_types=1);` in PHP files.
- Match the existing style: final classes where appropriate, enums for constrained values, readonly properties for immutable state, and typed constructor promotion.
- Keep public APIs and exception behavior stable unless the task requires breaking change.
- Prefer explicit validation and clear error messages for protocol and configuration inputs.
- Preserve typed PHPDoc for arrays such as `array<string,mixed>` and `list<string>`.
- Avoid one-letter variable names unless there is a strong local reason.

### Async and Amp rules

- This codebase is Amp-oriented. Do not introduce blocking waits in async paths.
- Prefer `Amp\delay()` over `sleep()` or `usleep()` in code that can run inside the event loop.
- Be careful with cancellation and timeout handling. Keep semantics consistent across related APIs.
- When polling, avoid tight spins; yield cooperatively.

### Protocol and parsing rules

- Treat all wire input as hostile or malformed until validated.
- Check lengths, negative values, and framing boundaries before slicing strings.
- Be conservative around header serialization. Never allow CR/LF injection into wire headers.
- Avoid legacy stateful functions when simpler explicit parsing is available.

### Testing standards

- When fixing a bug, add or update a test that exercises the bug path.
- Keep test names descriptive and behavior-oriented.
- If exception wording changes intentionally, update the tests in the same change.
- Prefer Behat for end-to-end documented workflows and PHPUnit for low-level protocol edges and exhaustive negative cases.

### Editing boundaries

- Do not edit `vendor/`.
- Do not manually edit generated JWT fixture files under `build/nats/jwt/` or `build/nats/jwt.conf`; use the fixture scripts.
- Do not hand-edit files only produced as test artifacts or generated output unless the task is explicitly about regeneration.
- Avoid broad reformatting unrelated files.

## Repository-Specific Guidance

### Auth and fixture cautions

- JWT integration depends on artifacts under `build/nats/jwt/` and `build/nats/jwt.conf`.
- `scripts/check-jwt-fixture.sh` regenerates fixtures before diffing them, so treat it as a mutating check.
- `scripts/regenerate-jwt-fixture.sh` may recreate the `nats-jwt` compose service if it is already running.

### Static analysis expectations

- PHPStan is configured at level 8 for both `src` and `tests`.
- Keep new code free of avoidable PHPStan noise.
- If a change introduces a new warning, fix it before finishing.

### Existing workflow expectations

- CI uses the same compose stack and readiness script as local integration runs.
- Scripts in `scripts/` are part of the supported workflow. Prefer reusing them over duplicating their logic in new commands.

## How Agents Should Work In This Repo

### Before editing

- Read the relevant source and nearby tests first.
- Check whether the task touches protocol parsing, auth, JetStream behavior, or integration fixtures, because those areas usually require broader validation.

### While editing

- Keep changes focused and minimal.
- Fix root causes, not only symptoms.
- Preserve naming, public API shape, and file organization unless a structural change is explicitly required.

### Before finishing

- Run the narrowest useful verification, then the broader suite required by the change type.
- Mention any tests you could not run.
- Mention any generated files or fixture scripts you had to invoke.

## Preferred Continuation Strategy For Future Agents

When continuing previous work:

1. Inspect the current diff before making more edits.
2. Read the touched source files and their closest tests.
3. Reuse existing scripts instead of inventing ad hoc commands.
4. If integration or fixture behavior is involved, prefer `composer test:e2e` over manually reproducing only part of the flow.
5. If JWT auth is involved, account for fixture validation and possible compose service recreation.

## Short Checklist

- Did you avoid editing generated fixture files by hand?
- Did you update or add tests for behavior changes?
- Did you use `delay()` instead of blocking sleeps in Amp code?
- Did you preserve strict typing and typed PHPDoc?
- Did you run the right verification command for the area you changed?
