<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Tests\Support\FakeTransport;

/**
 * Kills the surviving UnwrapTrim mutants in {@see \IDCT\NATS\Services\ServiceGroup::joinSubject()}.
 *
 * joinSubject() is private, but its result is the full endpoint subject persisted on the
 * ServiceEndpoint and exposed verbatim through Service::statsSnapshot()['endpoints'][i]['subject'].
 * That lets us pin the exact dot-trimming behaviour without any network or start() call.
 */
final class ServiceGroupMutationTest extends \PHPUnit\Framework\TestCase
{
    /**
     * @return array<int, string> The 'subject' of each registered endpoint, in registration order.
     */
    private function endpointSubjects(\IDCT\NATS\Services\Service $service): array
    {
        return array_map(
            static fn(array $endpoint): string => (string) ($endpoint['subject'] ?? ''),
            $service->statsSnapshot()['endpoints'] ?? [],
        );
    }

    /**
     * The group prefix must be trimmed of surrounding dots before being dot-joined to the subject.
     *
     * Real:    joinSubject('.svc.', 'echo') => trim('.svc.', '.')='svc' . '.' . 'echo' => 'svc.echo'
     * Mutated: $left = $prefix => '.svc.' . '.' . 'echo'                              => '.svc..echo'
     */
    public function testGroupPrefixIsTrimmedOfSurroundingDots(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());

        $service = $client->service('grp', '1.0.0');
        // Clean subject ('echo'): only the prefix trim on line 49 can change the result here.
        $service->addGroup('.svc.')->addEndpoint('echo', 'echo', static fn(NatsMessage $m): string => $m->payload);

        // kills UnwrapTrim @ line 49 (trim($prefix, '.') -> $prefix)
        self::assertSame(['svc.echo'], $this->endpointSubjects($service));
    }

    /**
     * The endpoint subject must be trimmed of surrounding dots before being dot-joined to the prefix.
     *
     * Real:    joinSubject('svc', '.echo.') => 'svc' . '.' . trim('.echo.', '.')='echo' => 'svc.echo'
     * Mutated: $right = $subject => 'svc' . '.' . '.echo.'                              => 'svc..echo.'
     */
    public function testEndpointSubjectIsTrimmedOfSurroundingDots(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());

        $service = $client->service('grp', '1.0.0');
        // Clean prefix ('svc'): only the subject trim on line 50 can change the result here.
        $service->addGroup('svc')->addEndpoint('echo', '.echo.', static fn(NatsMessage $m): string => $m->payload);

        // kills UnwrapTrim @ line 50 (trim($subject, '.') -> $subject)
        self::assertSame(['svc.echo'], $this->endpointSubjects($service));
    }
}
