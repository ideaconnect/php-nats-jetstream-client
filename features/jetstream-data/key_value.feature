Feature: JetStream KeyValue workflows
  KeyValue buckets should support watch, read, update, purge, and status operations.

  Scenario: Manage a KeyValue entry lifecycle
    Given I am connected to NATS
    And I have a random KeyValue bucket
    When I create the KeyValue bucket
    And I watch the KeyValue key "theme"
    And I put the KeyValue entry "theme" with value "dark"
    Then the KeyValue watch should observe "theme" with value "dark"
    And the KeyValue entry "theme" should have value "dark"
    When I delete the KeyValue entry "theme"
    Then the KeyValue entry "theme" should be marked as deleted

  Scenario: Run advanced KeyValue parity operations
    Given I am connected to NATS
    And I have a random KeyValue bucket
    When I create the KeyValue bucket
    And I put the KeyValue entry "username" with value "alice"
    And I update the KeyValue entry "username" from "alice" to "bob"
    And I put the KeyValue entry "email" with value "a@example.com"
    And I fetch all KeyValue entries
    Then the KeyValue bucket should contain "username" with value "bob"
    And the KeyValue bucket should contain "email" with value "a@example.com"
    When I purge the KeyValue entry "username"
    And I fetch all KeyValue entries
    Then the KeyValue bucket should not contain "username"
    And the KeyValue bucket should contain "email" with value "a@example.com"
    When I fetch the KeyValue status
    Then the KeyValue status should reference the current bucket