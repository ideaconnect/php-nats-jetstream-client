Feature: Username and password authentication
  User/password-authenticated NATS connections should follow the local auth fixture rules.

  Scenario: Connect with valid username and password
    When I connect with valid username and password authentication
    Then the authenticated connection should succeed

  Scenario: Reject an invalid password
    When I connect with an invalid password
    Then the connection should be rejected
