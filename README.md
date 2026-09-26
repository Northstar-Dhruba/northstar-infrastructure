# Northstar Infrastructure

`northstar-infrastructure` contains concrete technical adapters that fulfill ports owned by `northstar-application`.

## Implemented in Alpha v0.4

- `YahooFinanceMarketObservationSource` under `market_data` for live market observations.
- `YahooHistoricalMarketDataSource` for acquiring bounded historical daily OHLCV observations.
- `ExchangeCalendarTradingSessionResolver` under `exchanges` for exchange-accurate session completion resolution.
- `SQLiteHistoricalMarketDataRepository` for deterministic local historical research reads.
- `SQLiteHistoricalMarketDataStore` for atomic, idempotent historical research data persistence.
- Infrastructure-owned SQLite schema initialization for generic OHLCV observations.
- Yahoo Finance chart-data transport using the Python standard library.
- Translation of bounded daily market responses into Core/Application contracts.
- Explicit handling of provider unavailability, unknown symbols, incomplete observations, invalid values, timestamps, and required metadata.
- Deterministic adapter, repository, store, and exchange session tests; tests do not require live network access.

## Current Scope

The repository supports the one-asset market-observation integration and the Epic 6 historical research data pipeline (acquisition, storage, and retrieval). Historical reads are provider-independent and use a local SQLite store keyed by symbol, exchange, timeframe, and point in time.

Infrastructure owns provider communication, local storage schema, response parsing, and technical failure translation. It does not own recommendation policy, AssetAnalysis interpretation, or application orchestration. The generic repository and store can coexist with future derivatives-specific historical models without claiming that the current schema models futures or options contract identity.

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
