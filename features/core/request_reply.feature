Feature: Core request and reply flows
  Real request/reply workflows should succeed against the compose-backed fixture.

  Scenario: Request and reply across two connected clients
    Given I am connected to NATS
    And a second client is connected to NATS
    And I have a random request subject
    And the second client replies on my request subject with "pong"
    When I request "ping" on my request subject
    Then the request reply should be "pong"
