<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Services\ServiceEndpointHandlerInterface;
use IDCT\NATS\Tests\Support\FakeTransport;

/**
 * Targeted unit tests that pin the discovery INFO response shape and the class-handler
 * instantiation-failure exception code, killing the surviving Infection mutants in
 * src/Services/Service.php (chunk 3): the ArrayItem mutants on the INFO entry/response
 * keys (lines 748, 749, 750, 768) and the Increment/DecrementInteger mutants on the
 * exception code argument (line 810).
 */
final class Service_3MutationTest extends \PHPUnit\Framework\TestCase
{
    /** @return list<string> */
    private function infoAndPong(): array
    {
        return [
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ];
    }

    /**
     * Extracts and JSON-decodes the body of the first PUB to the given reply subject.
     *
     * @param list<string> $writes
     * @return array<string,mixed>
     */
    private function decodePublishedBody(array $writes, string $replySubject): array
    {
        $joined = implode('', $writes);
        $needle = 'PUB ' . $replySubject . ' ';
        $start = strpos($joined, $needle);
        self::assertNotFalse($start, "No PUB to {$replySubject} was written.");

        // PUB <subject> <len>\r\n<body>\r\n - skip past the header line to the body.
        $headerEnd = strpos($joined, "\r\n", $start);
        self::assertNotFalse($headerEnd);
        $bodyStart = $headerEnd + 2;
        $bodyEnd = strpos($joined, "\r\n", $bodyStart);
        self::assertNotFalse($bodyEnd);
        $body = substr($joined, $bodyStart, $bodyEnd - $bodyStart);

        /** @var array<string,mixed> $decoded */
        $decoded = json_decode($body, true, 512, JSON_THROW_ON_ERROR);

        return $decoded;
    }

    /**
     * Drives the service so it answers a single $SRV.INFO.<name> discovery request and returns the
     * decoded info_response payload.
     *
     * @return array<string,mixed>
     */
    private function fetchInfoResponse(): array
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            // $SRV.INFO.calc is the 5th discovery subscription (sid 5); request it with a reply inbox.
            "MSG \$SRV.INFO.calc 5 _INBOX.info 0\r\n\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        // Service name "calc" deliberately differs from the endpoint name "adder" so the endpoint
        // entry's "name" key is distinguishable from the top-level service "name" key.
        $service = $client->service('calc', '1.0.0', 'Adder service')
            ->addEndpoint('adder', 'calc.add', static fn(NatsMessage $m): string => $m->payload, 'qg');
        $service->start()->await();

        $client->processIncoming()->await();

        return $this->decodePublishedBody($transport->writes, '_INBOX.info');
    }

    /**
     * The INFO endpoint entry must carry the endpoint name/subject/queue_group under their literal
     * keys. The ArrayItem mutants replace `'key' => $value` with `'key' > $value`, which evaluates a
     * boolean appended under an int index and drops the named key entirely.
     */
    public function testInfoEndpointEntryUsesNamedKeys(): void
    {
        $info = $this->fetchInfoResponse();

        self::assertArrayHasKey('endpoints', $info);
        self::assertIsArray($info['endpoints']);
        $entry = $info['endpoints'][0] ?? null;
        self::assertIsArray($entry);

        // kills ArrayItem @ line 748 - the endpoint "name" key would vanish under the mutation.
        self::assertArrayHasKey('name', $entry);
        self::assertSame('adder', $entry['name']);

        // kills ArrayItem @ line 749 - the endpoint "subject" key would vanish under the mutation.
        self::assertArrayHasKey('subject', $entry);
        self::assertSame('calc.add', $entry['subject']);

        // kills ArrayItem @ line 750 - the endpoint "queue_group" key would vanish under the mutation.
        self::assertArrayHasKey('queue_group', $entry);
        self::assertSame('qg', $entry['queue_group']);

        // The mutation appends a boolean under int key 0 instead of the named key; ensure that
        // stray element never appears.
        self::assertArrayNotHasKey(0, $entry);
    }

    /**
     * The top-level info_response must carry the service description under the literal "description"
     * key. The ArrayItem mutant turns `'description' => $this->description` into a comparison whose
     * boolean result is appended under an int index, dropping the "description" key.
     */
    public function testInfoResponseCarriesDescriptionKey(): void
    {
        $info = $this->fetchInfoResponse();

        // kills ArrayItem @ line 768 - the top-level "description" key would vanish under the mutation.
        self::assertArrayHasKey('description', $info);
        self::assertSame('Adder service', $info['description']);
    }

    /**
     * When a class-string handler cannot be instantiated, the thrown InvalidArgumentException must
     * use code 0 and chain the underlying error as the previous exception. The Increment/Decrement
     * mutants change the code argument (0 -> 1 / 0 -> -1).
     */
    public function testClassHandlerInstantiationFailureUsesZeroCode(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('calc', '1.0.0');

        try {
            $service->addEndpoint('e', 'calc.e', ServiceMutationCtorArgHandler::class);
            self::fail('Expected addEndpoint() to reject a non-instantiable class handler.');
        } catch (\InvalidArgumentException $e) {
            self::assertStringContainsString('could not be instantiated', $e->getMessage());

            // kills DecrementInteger @ line 810 (0 -> -1) and IncrementInteger @ line 810 (0 -> 1).
            self::assertSame(0, $e->getCode());

            // The original instantiation error is chained as the previous exception (third ctor arg).
            self::assertInstanceOf(\Throwable::class, $e->getPrevious());
        }
    }
}

/**
 * Class handler with a required constructor argument: cannot be auto-instantiated, so resolveHandler()
 * throws the InvalidArgumentException whose code is asserted above.
 */
final class ServiceMutationCtorArgHandler implements ServiceEndpointHandlerInterface
{
    public function __construct(private readonly string $required) {}

    public function handle(NatsMessage $message): string
    {
        return $this->required . ':' . $message->payload;
    }
}
