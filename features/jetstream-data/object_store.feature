Feature: JetStream Object Store workflows
  Object Store buckets should support metadata watches, retrieval, callback streaming, listing, and tombstones.

  Scenario: Manage an Object Store object lifecycle
    Given I am connected to NATS
    And I have a random Object Store bucket
    When I create the Object Store bucket
    And I watch Object Store metadata updates
    And I store the object "logo.txt" with content "hello-object" and content type "text/plain"
    Then the Object Store watch should observe "logo.txt"
    And the object info for "logo.txt" should include content type "text/plain"
    And downloading the object "logo.txt" should return "hello-object"
    And streaming the object "logo.txt" to a callback should return "hello-object"
    When I list the stored objects
    Then the object list should include "logo.txt"
    When I fetch the Object Store status
    Then the Object Store status should reference the current bucket
    When I delete the object "logo.txt"
    Then the object "logo.txt" should be marked as deleted