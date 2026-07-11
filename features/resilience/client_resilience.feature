Feature: Live client resilience workflows
  The client should surface live-server edge cases and runtime guarantees through end-to-end scenarios.

  Scenario: no_echo suppresses self-published messages
    Given I have a random subject
    When I connect with no_echo enabled and subscribe to my subject
    And I publish "self" to my subject from the no_echo client
    Then I should not receive my own no_echo message

  Scenario: Request without responders surfaces a no responders error
    Given I am connected to NATS
    And I have a random subject
    When I request "hello" on my subject without responders
    Then the request should fail with a no responders error

  Scenario: Request timeout surfaces a timeout error after a responder receives the request
    Given I am connected to NATS
    And a second client is connected to NATS
    And I have a random request subject
    When a silent responder is subscribed on my request subject
    And I request "hello" on my request subject and wait for timeout
    Then the request should fail with a timeout error
    And the silent responder should have received the request

  Scenario: Drain flushes in-flight delivery before closing the connection
    Given I have a random subject
    When I drain a subscriber after publishing an in-flight message
    Then draining should flush the in-flight message and close the client

  Scenario: Wildcard subscriptions only receive matching subjects
    Given I am connected to NATS
    And a second client is connected to NATS
    And I have a random wildcard subscription pattern and subjects
    When I subscribe to the wildcard pattern and publish matching and non-matching subjects
    Then I should receive only the matching wildcard subjects and payloads

  Scenario: Oversized publish is rejected before writing to the server
    Given I am connected to NATS
    And I have a random subject
    When I publish a payload larger than the server max payload
    Then the oversized publish should be rejected by the client

  Scenario: Auto-unsubscribe delivers remaining messages up to the maximum then stops
    Given I am connected to NATS
    And I have a random subject
    When I subscribe to my subject
    And I unsubscribe from my subject with a maximum of 2 messages
    And I publish "auto-1" to my subject
    And I publish "auto-2" to my subject
    And I publish "auto-3" to my subject
    Then I should receive exactly 2 messages