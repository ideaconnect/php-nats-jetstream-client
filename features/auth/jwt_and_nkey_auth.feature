Feature: JWT, NKey, and credentials-file authentication
  Decentralized auth flows should succeed against the compose-backed fixtures.

  Scenario: Connect with JWT nonce authentication
    When I connect with JWT nonce authentication
    Then the authenticated connection should succeed

  Scenario: Connect with standalone NKey authentication
    When I connect with standalone NKey authentication
    Then the authenticated connection should succeed

  Scenario: Connect with a generated credentials file
    When I connect with a generated credentials file
    Then the authenticated connection should succeed
