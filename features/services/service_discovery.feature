Feature: NATS services discovery and validation workflows
  The services framework should expose endpoint replies, discovery subjects, and schema validation over a live server.

  Scenario: Start an echo service and reply to requests
    Given I am connected to NATS
    And I have a random service name and echo subject
    When I start the echo service
    And I request "hello" from the echo service
    Then the echo service reply should be "reply:hello"

  Scenario: Expose discovery payloads for schema and plain endpoints
    Given I am connected to NATS
    And I have a random service with schema and plain subjects
    When I start the discovery service with schema and plain endpoints
    And I query the service discovery subjects
    Then the ping discovery response should describe the current service
    And the info discovery response should list 2 endpoints
    And the stats discovery response should list 2 endpoints
    And the schema discovery response should include schema only for the schema endpoint

  Scenario: Validate requests and emit observer correlation metadata
    Given I am connected to NATS
    And I have a random service with schema and plain subjects
    When I start the validated service with observers
    And I send invalid and valid requests to the validated service
    Then the invalid service response should be a validation error
    And the valid service response should echo the valid request
    And the service stats should record 2 requests and 1 errors
    And the service observers should capture both correlation ids