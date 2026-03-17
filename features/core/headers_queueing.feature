Feature: Core headers and queueing workflows
  Core NATS helper examples from the README should work end to end against the live fixture.

  Scenario: Publish and request with headers while reading server info
    Given I am connected to NATS
    And a second client is connected to NATS
    And I have a random subject
    And I have a random request subject
    When I subscribe to my subject and record headers
    And I publish "hello" with headers to my subject
    And I process incoming messages
    And the second client replies with captured headers on my request subject
    And I request "hello" with headers on my request subject
    Then the published message should include the custom headers
    And the request handler should receive the custom request header
    And the request reply should be "ok"
    And server info should be available

  Scenario: Queue group subscribers distribute messages without duplication
    Given I have a random subject
    When two worker clients subscribe to my subject with a shared queue group
    And I publish 20 queue messages to my subject
    Then the queue group should distribute 20 messages without duplicates

  Scenario: Polling subscription queue supports fetch, next, and fetchAll
    Given I am connected to NATS
    And a second client is connected to NATS
    And I have a random subject
    When I create a polling subscription queue for my subject
    And the second client publishes "one", "two", and "three" to my subject
    And I fetch queued messages using fetch, next, and fetchAll
    Then the polling subscription queue should return "one", "two", and "three"