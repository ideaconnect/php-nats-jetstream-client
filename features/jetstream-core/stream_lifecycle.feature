Feature: JetStream stream lifecycle smoke coverage
  Real JetStream workflows should succeed against the compose-backed fixture.

  Scenario: Fetch account info and manage a stream lifecycle
    Given I am connected to NATS
    And I have a random JetStream stream and subject
    When I fetch JetStream account info
    And I create the JetStream stream for my subject
    Then the JetStream account info request should succeed
    And the JetStream stream should be available
    When I delete the JetStream stream
    Then the JetStream stream should be removed
