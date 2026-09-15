# Northstar Infrastructure

`northstar-infrastructure` contains concrete technical adapters that fulfill ports owned by `northstar-application`.

## Implemented in Alpha v0.4

- `YahooFinanceMarketObservationSource` under `market_data`.
- Yahoo Finance chart-data transport using the Python standard library.
- Translation of one bounded daily market response into the existing `MarketObservationContext` contract.
- Explicit handling of provider unavailability, unknown symbols, incomplete observations, invalid values, timestamps, and required metadata.
- Deterministic adapter tests using fake transport; tests do not require live network access.

## Current Scope

The repository currently supports the single market-observation integration required by the Northstar one-asset intelligence workflow. The adapter is wired at the API composition root and implements the Application `MarketObservationSource` port.

Infrastructure owns provider communication, request construction, response parsing, and provider failure translation. It does not own recommendation policy, AssetAnalysis interpretation, or application orchestration.

## Dependency Direction

Infrastructure depends inward on `northstar-application` ports and `northstar-core` Domain contracts. Core and Application do not depend on Infrastructure implementations.

## Future Infrastructure

The following areas are architectural placeholders or future capabilities, not implemented Alpha adapters:

- broker and exchange integrations;
- persistence and databases;
- messaging and event transports;
- REST/WebSocket infrastructure beyond the current provider transport;
- FIX integrations;
- configuration and secret-management services;
- additional market-data providers.

## Development

Use `uv sync`, then run:

```powershell
uv run ruff check .
uv run ruff format --check .
uv run python -m pytest
uv lock --check
```

See [DEVELOPMENT.md](DEVELOPMENT.md) and [CONTRIBUTING.md](CONTRIBUTING.md) for repository workflow and engineering rules.
