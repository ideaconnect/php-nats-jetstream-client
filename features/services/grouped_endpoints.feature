Feature: NATS services grouped endpoint workflows
  Grouped service endpoints should use hierarchical subject prefixes and report those subjects in service stats.

  Scenario: Dispatch requests across grouped echo endpoints
    Given I am connected to NATS
    And I have a random grouped service subject hierarchy
    When I start the grouped echo service
    And I request both grouped service endpoints with payload "hello"
    Then the grouped service replies should be "v1:hello" and "v2:hello"
    And the grouped service stats should list both grouped subjects