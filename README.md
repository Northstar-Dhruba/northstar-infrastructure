# Northstar Infrastructure

**Northstar Platform Infrastructure Layer — Technical Adapters and External Integrations**

## Purpose

`northstar-infrastructure` owns concrete technical implementations and external integrations for Northstar Platform v1.1.0. It provides technical adapters that fulfill Application-owned Port abstractions.

It exists to encapsulate all technical mechanisms (databases, protocols, transport formats, messaging systems, external APIs) so that business rules in `northstar-core` and orchestration logic in `northstar-application` remain pure and technology-agnostic.

## Responsibilities

- **Broker Adapters:** Implementing broker communication and order lifecycle adapters.
- **Exchange Adapters:** Implementing exchange venue connectivity and market feed integration.
- **Persistence:** Implementing repository and unit-of-work abstractions for database storage.
- **Messaging:** Implementing message transports, event publishing, and bus integrations.
- **REST & WebSocket:** Implementing HTTP/REST and real-time streaming clients/servers.
- **FIX Protocol:** Implementing FIX protocol engine integrations and session management.
- **Configuration:** Managing environment configuration, secret loading, and runtime settings.

## Adapter Philosophy

- **Ports and Adapters:** Application Layer defines the Port interface; Infrastructure provides the Adapter implementation.
- **Data Translation:** Infrastructure adapters translate technical protocol messages (FIX tags, JSON, DB rows) into/from Application and Domain contracts.
- **No Business Logic in Adapters:** Adapters translate technical mechanics and call Domain/Application boundaries; they do NOT evaluate business invariants or make trading decisions.

## Dependency Direction

- **Inward Dependency:** `northstar-infrastructure` depends inward on `northstar-application` ports and `northstar-core` Domain abstractions.
- **Zero Inward Leakage:** Neither `northstar-core` nor `northstar-application` ever imports or depends on concrete classes in `northstar-infrastructure`.
- **Wired at Composition Root:** Infrastructure adapters are instantiated and bound to Application ports at the application composition root (e.g. `northstar-api` or startup CLI).

## Repository Structure

```text
src/
    northstar_infrastructure/
        brokers/        # Broker-specific adapter implementations
        exchanges/      # Exchange venue adapter implementations
        persistence/    # Database repositories & storage adapters
        messaging/      # Message bus & queue transport adapters
        rest/           # HTTP/REST clients and transport adapters
        websocket/      # WebSocket streaming adapters
        fix/            # FIX protocol engine integration
        configuration/  # Setting loaders & environment management
tests/                  # Automated test suite matching Northstar standards
docs/                   # Infrastructure layer documentation
```

## Engineering Rules

1. **Implement Ports Only:** Every adapter must implement an explicit Application-owned port contract.
2. **Translation at Boundary:** Perform technical translation at the edge. Keep internal types pure.
3. **Constructor Injection:** Inject technical clients and connection factories through constructors.
4. **Clean Architecture:** Respect the dependency inversion principle without exception.
5. **Isolated Testing:** Test adapters against mock/stub technical endpoints or embedded test containers.

## Governance & Reference Documents

- [Architecture Handbook v2.0](../docs/architecture/Architecture-Handbook-v2.0.md)
- [Application Layer Design Specification v1.0](../docs/architecture/Application-Layer-Design-Specification-v1.0.md)
- [Northstar Architecture v2.0](../docs/architecture/Northstar-Architecture-v2.0.md)
