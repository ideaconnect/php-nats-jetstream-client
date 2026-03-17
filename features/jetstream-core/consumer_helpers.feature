Feature: JetStream consumer helper workflows
  README consumer helper examples should deliver, redeliver, and pause correctly against the live JetStream fixture.

  Scenario: Pull fetch and ACK returns the published payload
    Given I am connected to NATS
    And I have a random JetStream stream with primary and secondary subjects
    And I have a random durable consumer name
    When I fetch and ACK the next pull message "pull-event"
    Then the JetStream helper should receive "pull-event"

  Scenario: Delayed NAK redelivers a pull message
    Given I am connected to NATS
    And I have a random JetStream stream with primary and secondary subjects
    And I have a random durable consumer name
    When I redeliver a pull message with delayed NAK and then ACK it
    Then the JetStream helper should receive "redeliver-event"

  Scenario: In-progress heartbeats delay redelivery and TERM stops later redelivery
    Given I am connected to NATS
    And I have a random JetStream stream with primary and secondary subjects
    And I have a random durable consumer name
    When I exercise in-progress heartbeats and TERM on a pull consumer
    Then the JetStream helper should receive "wpi-event"

  Scenario: Durable push helper delivers a live message
    Given I am connected to NATS
    And I have a random JetStream stream with primary and secondary subjects
    And I have a random durable consumer name
    When I subscribe with the durable push consumer helper and publish "push-event"
    Then the JetStream helper should receive "push-event"

  Scenario: Ephemeral pull helper fetches and ACKs a live message
    Given I am connected to NATS
    And I have a random JetStream stream with primary and secondary subjects
    When I create an ephemeral pull consumer and fetch "ephemeral-event"
    Then the JetStream helper should receive "ephemeral-event"

  Scenario: Ephemeral push helper delivers a live message
    Given I am connected to NATS
    And I have a random JetStream stream with primary and secondary subjects
    When I subscribe with the ephemeral push consumer helper and publish "ephemeral-push-event"
    Then the JetStream helper should receive "ephemeral-push-event"

  Scenario: Ordered consumer still delivers after a prior non-matching stream message
    Given I am connected to NATS
    And I have a random JetStream stream with primary and secondary subjects
    When I subscribe with the ordered consumer helper and publish "ordered-event" after a non-matching message
    Then the JetStream helper should receive "ordered-event"

  Scenario: Pause and resume suppresses then restores pull delivery
    Given I am connected to NATS
    And I have a random JetStream stream with primary and secondary subjects
    And I have a random durable consumer name
    When I pause the current consumer, verify no delivery, then resume it
    Then the JetStream helper should receive "paused-event"

  Scenario: Fetch batch returns all requested messages
    Given I am connected to NATS
    And I have a random JetStream stream with primary and secondary subjects
    And I have a random durable consumer name
    When I fetch a batch of 5 JetStream messages and ACK them
    Then the fetched batch should contain 5 JetStream messages

  Scenario: Pull-consumer iteration processes messages across batched fetches
    Given I am connected to NATS
    And I have a random JetStream stream with primary and secondary subjects
    And I have a random durable consumer name
    When I process pull-consumer iteration for 5 JetStream messages in batches of 2
    Then pull-consumer iteration should process 5 messages total