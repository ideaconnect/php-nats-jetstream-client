Feature: JetStream management workflows
  README stream-management examples should work against the live JetStream fixture.

  Scenario: Update a stream and inspect consumer and stream listings
    Given I am connected to NATS
    And I have a random JetStream stream with primary and secondary subjects
    And I have a random durable consumer name
    When I create the JetStream stream for my subject
    And I update the JetStream stream to include the secondary subject
    And I create a durable consumer for the primary subject
    And I fetch the durable consumer info
    And I list consumers for the current stream
    And I list streams
    Then the JetStream stream should include both configured subjects
    And the durable consumer info should match the current stream and consumer
    And the JetStream consumer list should include the current consumer
    And the JetStream stream list should include the current stream

  Scenario: Direct get returns the last published stream message and purge clears the stream
    Given I am connected to NATS
    And I have a random JetStream stream with primary and secondary subjects
    When I create the JetStream stream for my subject
    And I publish "direct-get-event" to the primary JetStream subject
    And I fetch the stream message using the last publish sequence
    Then the direct stream get should return "direct-get-event"
    When I purge the current JetStream stream
    Then the current JetStream stream should have no stored messages

  Scenario: Typed stream and consumer configuration persist in JetStream
    Given I am connected to NATS
    And I have a random JetStream stream with primary and secondary subjects
    And I have a random durable consumer name
    When I create the JetStream stream and consumer with typed configuration
    Then the typed JetStream configuration should persist on the stream and consumer