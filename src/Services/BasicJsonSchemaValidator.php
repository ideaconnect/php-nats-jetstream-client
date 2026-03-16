<?php

declare(strict_types=1);

namespace IDCT\NATS\Services;

use IDCT\NATS\Core\NatsMessage;

/**
 * Minimal JSON Schema-like validator for service request payloads.
 */
final class BasicJsonSchemaValidator implements ServiceSchemaValidatorInterface
{
    /**
     * @param array<string,mixed> $schema
     */
    public function validate(NatsMessage $message, array $schema): ?string
    {
        $payload = json_decode($message->payload, true);
        if (json_last_error() !== JSON_ERROR_NONE) {
            return 'payload is not valid JSON';
        }

        return $this->validateValue($payload, $schema, '$');
    }

    /**
     * @param mixed $value
     * @param array<string,mixed> $schema
     */
    private function validateValue(mixed $value, array $schema, string $path): ?string
    {
        $expectedType = $schema['type'] ?? null;
        if (is_string($expectedType)) {
            $typeError = $this->validateType($value, $expectedType, $path);
            if ($typeError !== null) {
                return $typeError;
            }
        }

        if (($schema['type'] ?? null) === 'object' && is_array($value)) {
            $required = $schema['required'] ?? [];
            if (is_array($required)) {
                foreach ($required as $property) {
                    if (is_string($property) && !array_key_exists($property, $value)) {
                        return sprintf('%s.%s is required', $path, $property);
                    }
                }
            }

            $properties = $schema['properties'] ?? [];
            if (is_array($properties)) {
                foreach ($properties as $property => $propertySchema) {
                    if (!is_string($property) || !is_array($propertySchema)) {
                        continue;
                    }

                    if (!array_key_exists($property, $value)) {
                        continue;
                    }

                    $error = $this->validateValue($value[$property], $propertySchema, $path . '.' . $property);
                    if ($error !== null) {
                        return $error;
                    }
                }
            }
        }

        return null;
    }

    private function validateType(mixed $value, string $expectedType, string $path): ?string
    {
        $actualType = get_debug_type($value);

        $isValid = match ($expectedType) {
            'string' => is_string($value),
            'integer' => is_int($value),
            'number' => is_int($value) || is_float($value),
            'boolean' => is_bool($value),
            'array' => is_array($value) && array_is_list($value),
            'object' => is_array($value),
            'null' => $value === null,
            default => true,
        };

        if ($isValid) {
            return null;
        }

        return sprintf('%s must be %s, got %s', $path, $expectedType, $actualType);
    }
}
