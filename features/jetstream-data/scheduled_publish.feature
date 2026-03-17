Feature: JetStream scheduled publish workflows
  Scheduled JetStream publishing should deliver a delayed message to the configured target subject.

  Scenario: Publish a delayed message through the scheduler
    Given I am connected to NATS
    And I have a random scheduled JetStream stream and subjects
    When I create the JetStream stream with scheduling enabled
    And I publish the scheduled message "scheduled-event"
    Then the scheduled publish should be acknowledged for my stream
    And the scheduled message should become visible in the stream