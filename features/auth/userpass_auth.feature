Feature: Username and password authentication
  User/password-authenticated NATS connections should follow the local auth fixture rules.

  Scenario: Connect with valid username and password
    When I connect with valid username and password authentication
    Then the authenticated connection should succeed

  Scenario: Reject an invalid password
    When I connect with an invalid password
    Then the connection should be rejected

  Scenario: A request the server refuses for a publish permission fails at once
    Given a user who may not publish to "permissions.refused.>" is connected to the username and password server
    When that user sends a request to "permissions.refused.echo"
    Then it should fail at once with a permission violation for "permissions.refused.echo"
    And a request from that user to a subject it may publish to should still be answered

  Scenario: A JetStream API call the server refuses for a publish permission fails at once
    Given a user who may not publish to "permissions.refused.>" is connected to the username and password server
    When that user asks for the info of a consumer
    Then it should fail at once with a permission violation for the consumer info subject
    And that user can still read the stream's info

  Scenario: A pull the server refuses for a publish permission fails at once
    Given a user who may not publish to "permissions.refused.>" is connected to the username and password server
    When that user pulls from a consumer with a message waiting
    Then it should fail at once with a permission violation for the pull subject
    And the message should still be waiting for a user who may pull
