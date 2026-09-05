# Contributing to Northstar Infrastructure

Thank you for contributing to `northstar-infrastructure`. This repository contains the Infrastructure Layer of the Northstar Platform.

## Architectural Rules

Before making any contributions, you must be familiar with:

- **Architecture Handbook v2.0**
- **Application Layer Design Specification v1.0**
- **Northstar Architecture v2.0**

### Core Constraints

1. **Adapters Only:** Every class in `northstar-infrastructure` is an adapter that implements a Port interface defined in `northstar-application`.
2. **No Business Logic:** Never embed business rules, trading decisions, or validation invariants in infrastructure adapters.
3. **Inward Dependencies Only:** Infrastructure depends on `northstar-application` and `northstar-core`. Core and Application NEVER depend on Infrastructure.
4. **Constructor Injection:** Pass connection details, clients, and configuration via constructors.
5. **Isolate Protocol Types:** Do not leak FIX tags, database schemas, or HTTP status structures into Application or Domain models.

## Development Workflow

1. Ensure Python 3.13 and `uv` are installed.
2. Clone the repository and run `uv sync`.
3. Format and lint code with `uv run ruff check .` and `uv run ruff format --check .`.
4. Run tests with `uv run pytest`.
5. Ensure all PRs pass GitHub Actions CI.
