# Development Guide — Northstar Infrastructure

This document provides setup and development instructions for working with `northstar-infrastructure`.

## Prerequisites

- **Python:** 3.13
- **Package & Environment Manager:** `uv` (>= 0.5.0)

## Getting Started

1. **Setup Virtual Environment & Install Dependencies:**

    ```bash
    uv sync
    ```

2. **Run Linter Checks:**

    ```bash
    uv run ruff check .
    uv run ruff format --check .
    ```

3. **Auto-format Code:**

    ```bash
    uv run ruff format .
    ```

4. **Run Test Suite:**
    ```bash
    uv run pytest
    ```

## Directory Structure Overview

- `src/northstar_infrastructure/`: Package source code.
    - `brokers/`: Broker integration adapters.
    - `exchanges/`: Exchange venue integration adapters.
    - `persistence/`: Database repositories and unit-of-work implementations.
    - `messaging/`: Message bus and event distribution transport adapters.
    - `rest/`: REST API clients and transport layers.
    - `websocket/`: Real-time WebSocket streaming adapters.
    - `fix/`: FIX protocol engine session management and messaging adapters.
    - `configuration/`: Environment configuration management.
- `tests/`: Automated unit and integration tests.
- `docs/`: Adapter specifications and technical integration notes.
