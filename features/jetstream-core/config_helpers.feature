Feature: JetStream configuration helper workflows
  Republish and subject-transform helper configurations should produce live JetStream behavior against the fixture server.

  Scenario: Republish forwards matching messages to the configured destination subject
    Given I am connected to NATS
    And a second client is connected to NATS
    And I have a random JetStream stream with primary and secondary subjects
    When I create a stream with republish from the primary to the secondary subject
    And the second client subscribes to the secondary subject for republished messages
    And I publish "republished-event" to the republished primary subject
    Then the republished subscriber should receive "republished-event" on the secondary subject

  Scenario: Subject transform stores the message under the configured destination subject
    Given I am connected to NATS
    And I have a random JetStream stream with primary and secondary subjects
    When I create a stream with a subject transform from the primary to the secondary subject
    And I publish "transformed-event" to the transformed primary subject
    And I fetch the transformed stream message by the last publish sequence
    Then the transformed stream message should be stored under the secondary subject with payload "transformed-event"

  Scenario: Source filtering replicates only matching origin messages
    Given I am connected to NATS
    And I have a random JetStream stream with primary and secondary subjects
    When I create an origin stream and a sourced stream filtered to the primary subject
    Then the sourced stream should contain only "sourced-event" from the primary subject

  Scenario: Mirror replication copies origin messages without local subjects
    Given I am connected to NATS
    And I have a random JetStream stream with primary and secondary subjects
    When I create an origin stream and a mirror stream from it
    And I publish "mirrored-event" to the mirrored origin subject
    Then the mirror stream should contain "mirrored-event"