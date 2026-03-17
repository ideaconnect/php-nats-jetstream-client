Feature: Core connection and publish/subscribe flows
  Real NATS workflows should succeed against the compose-backed fixture.

  Scenario: Publish and subscribe with a single client
    Given I am connected to NATS
    And I have a random subject
    When I subscribe to my subject
    And I publish "hello from behat" to my subject
    And I process incoming messages
    Then I should receive the message "hello from behat"
