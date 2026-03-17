Feature: Token authentication
  Token-authenticated NATS connections should follow the local auth fixture rules.

  Scenario: Connect with the configured valid token
    When I connect with valid token authentication
    Then the authenticated connection should succeed

  Scenario: Reject an invalid token
    When I connect with invalid token authentication
    Then the connection should be rejected
