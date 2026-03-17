Feature: TLS authentication
  TLS-secured NATS connections should obey the compose-backed fixture requirements.

  Scenario: Connect with TLS handshake-first and client credentials
    When I connect with TLS handshake-first authentication
    Then the authenticated connection should succeed

  Scenario: Reject a TLS client without a certificate
    When I attempt a TLS connection without a client certificate
    Then the connection should be rejected
