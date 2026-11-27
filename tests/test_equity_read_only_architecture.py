"""EI-7D: architecture of the read-only readers -- static source checks and runtime facts.

The three EI-7D modules are Infrastructure-only, depend on merged Application
ports and Core value objects and on the accepted pure verified decodings, and
are read-only by construction: one ``mode=ro`` connection path with a
confirmed ``query_only``, SQL that only reads, no schema initializer, no
write-capable adapter, no write use case, no clock, no provider, broker,
monitor, API or Web dependency, no catalog or unrestricted listing. Runtime
behaviour is proven by the other EI-7D suites; these checks keep the source
honest and are written not to reject legitimate shared pure decoder imports.
"""

from __future__ import annotations

import ast
import inspect
from pathlib import Path
from types import ModuleType

import pytest

from northstar_infrastructure import persistence
from northstar_infrastructure.persistence import sqlite_equity_evidence_read_only as evidence
from northstar_infrastructure.persistence import sqlite_equity_read_only as connection
from northstar_infrastructure.persistence import (
    sqlite_equity_recommendation_read_only as recommendation,
)

MODULES = (connection, recommendation, evidence)
READERS = (
    persistence.SQLiteReadOnlyEquityRecommendationIssuanceReader,
    persistence.SQLiteReadOnlyEquityRecommendationLifecycleReader,
    persistence.SQLiteReadOnlyEquityRecommendationGenerationReader,
    persistence.SQLiteReadOnlyEquityIntradayResearchTimelineReader,
    persistence.SQLiteReadOnlyEquityCandleEvidenceSnapshotReader,
)
_MUTATING = (
    *("INSERT", "UPDATE", "DELETE", "REPLACE", "CREATE", "DROP", "ALTER", "ATTACH"),
    *("DETACH", "VACUUM", "REINDEX", "ANALYZE", "COMMIT", "SAVEPOINT", "RELEASE"),
)


def _tree(module: ModuleType) -> ast.Module:
    return ast.parse(Path(inspect.getfile(module)).read_text(encoding="utf-8"))


def _docstrings(tree: ast.Module) -> set[int]:
    nodes = [tree, *(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef | ast.ClassDef))]
    found = set()
    for node in nodes:
        body = getattr(node, "body", [])
        if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
            found.add(id(body[0].value))
    return found


def _code_strings(module: ModuleType) -> list[str]:
    """Every string constant in the module's code -- docstrings excluded."""
    tree = _tree(module)
    skip = _docstrings(tree)
    return [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in skip
    ]


def _imports(module: ModuleType) -> tuple[set[str], set[str]]:
    """(imported module names, imported names) of ``module``."""
    tree = _tree(module)
    modules: set[str] = set()
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
            names |= {alias.name for alias in node.names}
        elif isinstance(node, ast.Import):
            modules |= {alias.name for alias in node.names}
    return modules, names


def _calls(module: ModuleType) -> list[str]:
    """Every dotted call target in the module, e.g. ``sqlite3.connect``."""
    found = []
    for node in ast.walk(_tree(module)):
        if isinstance(node, ast.Call):
            target, parts = node.func, []
            while isinstance(target, ast.Attribute):
                parts.append(target.attr)
                target = target.value
            if isinstance(target, ast.Name):
                parts.append(target.id)
            found.append(".".join(reversed(parts)))
    return found


@pytest.mark.parametrize("module", MODULES, ids=lambda m: m.__name__.rsplit(".", 1)[-1])
def test_every_module_is_infrastructure_persistence(module: ModuleType) -> None:
    assert module.__name__.startswith("northstar_infrastructure.persistence.")


@pytest.mark.parametrize("module", MODULES, ids=lambda m: m.__name__.rsplit(".", 1)[-1])
def test_the_sql_written_in_each_module_only_reads(module: ModuleType) -> None:
    starts = {"SELECT", "WITH", "PRAGMA", "BEGIN", "ROLLBACK", "WHERE", *_MUTATING}
    sql = [s for s in _code_strings(module) if s.split() and s.split()[0].upper() in starts]
    # The evidence readers write no SQL at all: only the reused EI-2B/EI-3C statements.
    assert bool(sql) is (module is not evidence), [module.__name__, sql]
    for text in sql:
        words = set(text.upper().replace("(", " ").replace(",", " ").split())
        assert not words & set(_MUTATING), text
        assert "LIMIT" not in words, text  # never a prefix of a longer answer


def test_the_runtime_sql_of_the_readers_only_reads_and_is_always_scoped() -> None:
    statements = {
        "session declarations at W": recommendation._SESSION_DECLARATIONS_AT,  # noqa: SLF001
        "session issuances at W": recommendation._SESSION_ISSUANCES_AT,  # noqa: SLF001
        "session count at W": recommendation._SESSION_COUNT_AT,  # noqa: SLF001
        "disposition by key": recommendation._SELECT_DISPOSITION,  # noqa: SLF001
        "attempt by identity": recommendation._SELECT_ATTEMPT,  # noqa: SLF001
        "timeline by identity": evidence._SELECT_TIMELINE,  # noqa: SLF001
    }
    for name, statement in statements.items():
        upper = statement.upper()
        assert upper.lstrip().startswith("SELECT"), name
        assert " WHERE " in upper and " LIMIT " not in upper, name  # never a global listing
        assert not set(upper.split()) & set(_MUTATING), name


def test_the_one_connection_path_is_mode_ro_with_query_only_and_no_fallback() -> None:
    strings = [s for m in MODULES for s in _code_strings(m)]
    assert "?mode=ro" in "".join(_code_strings(connection))
    assert "PRAGMA query_only = ON" in _code_strings(connection)
    joined = "\n".join(strings)
    for forbidden in ("mode=rw", "mode=rwc", "immutable", "journal_mode", "nolock"):
        assert forbidden not in joined, forbidden

    def connects(module: ModuleType) -> list[str]:
        return [c for c in _calls(module) if c.split(".")[-1] in ("connect", "connect_read_only")]

    assert connects(connection) == ["sqlite3.connect", "connect_read_only"]  # one path
    assert connects(recommendation) == [] and connects(evidence) == []  # only through it


@pytest.mark.parametrize("module", MODULES, ids=lambda m: m.__name__.rsplit(".", 1)[-1])
def test_no_module_reaches_a_writer_initializer_or_write_use_case(module: ModuleType) -> None:
    modules, names = _imports(module)
    assert not {n for n in names if n.startswith("initialize_")}
    assert "_connect" not in names and "_write" not in names and "_commit" not in names
    for name in names:
        assert not (name.startswith("SQLite") and name.endswith(("Store", "Authority"))), name
        assert not name.startswith(("require_verified", "Verified", "Record", "Issue",
                                    "Declare", "Monitor", "Generate", "Publish")), name  # fmt: skip
        assert not name.endswith(("Writer", "Recorder", "Store")), name
    forbidden_modules = {
        "time", "threading", "multiprocessing", "sched", "socket", "random", "subprocess",
        "urllib", "http", "requests", "fastapi", "databento", "asyncio",
    }  # fmt: skip
    assert not {m.split(".")[0] for m in modules} & forbidden_modules
    for name in modules:
        for word in (
            *("broker", "market_data", "monitor", "catalog", "policy", "api", "web"),
            *("northstar_api", "northstar_web", "messaging", "rest", "websocket"),
        ):
            assert word not in name.split("."), (name, word)


@pytest.mark.parametrize("module", MODULES, ids=lambda m: m.__name__.rsplit(".", 1)[-1])
def test_no_module_reads_a_clock(module: ModuleType) -> None:
    calls = _calls(module)
    for clock in ("now", "utcnow", "today", "time", "monotonic", "perf_counter"):
        assert not any(call.split(".")[-1] == clock for call in calls), clock


def test_the_readers_reuse_the_accepted_verified_decodings() -> None:
    _, recommendation_names = _imports(recommendation)
    _, evidence_names = _imports(evidence)
    assert {
        "_consistent_ledger",
        "_stored_issuance",
        "_stored_declaration",
        "_parent",
        "_window",
        "_stored_lifecycle",
        "_stored_disposition",
        "_stored_attempt",
    } <= recommendation_names
    assert {"_stored_timeline", "_stored_snapshot"} <= evidence_names
    assert recommendation._stored_issuance.__module__.endswith("issuance")  # noqa: SLF001
    assert evidence._stored_snapshot.__module__.endswith("candle_evidence")  # noqa: SLF001


def test_every_reader_uses_the_read_only_transaction_and_holds_only_a_path(tmp_path: Path) -> None:
    for reader in READERS:
        instance = reader(tmp_path / "x.sqlite3")
        assert set(vars(instance)) == {"_database_path"}, reader
        source = inspect.getsource(reader)
        assert "_connect(" not in source and "sqlite3.connect" not in source, reader
    assert list(tmp_path.iterdir()) == []


def test_the_recorded_session_reader_is_the_only_discovery_and_takes_one_session() -> None:
    signature = inspect.signature(
        persistence.SQLiteReadOnlyEquityRecommendationIssuanceReader.recorded_session_issuances
    )
    assert list(signature.parameters) == [
        "self",
        "exchange_code",
        "trading_date",
        "limit",
        "watermark",
    ]
    for reader in READERS:  # no list-all, date range, strategy filter or pagination
        for name in dir(reader):
            assert not name.startswith(("list", "all_", "browse", "search", "page", "scan")), name


def test_the_readers_are_exported_and_the_existing_adapters_are_unchanged_exports() -> None:
    for reader in READERS:
        assert reader.__name__ in persistence.__all__
        assert getattr(persistence, reader.__name__) is reader
    for existing in (
        "SQLiteEquityRecommendationIssuanceAuthority",
        "SQLiteEquityRecommendationLifecycleStore",
        "SQLiteEquityRecommendationGenerationStore",
        "SQLiteEquityIntradayResearchTimelineStore",
        "SQLiteEquityCandleEvidenceSnapshotStore",
        "SQLiteEquityCandleEvidenceSnapshotCatalog",
    ):
        assert existing in persistence.__all__
