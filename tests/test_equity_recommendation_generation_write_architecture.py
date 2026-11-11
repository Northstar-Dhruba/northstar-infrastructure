"""EI4-D11-W4-I: the Infrastructure generation write boundary as an architecture rule.

ADR-023's final adapter surface, pinned three ways:

- the adapter class: exactly the Application port's two reads and two
  authorized writes, no raw ``record_*``, no public alias of a private
  persistence helper, and no constructor switch;
- the write entries: each calls the Application-owned validator first, with
  no branch around it, and no SQLite connection opens before it returns;
- a syntax-tree scan of Infrastructure production source
  (``src/northstar_infrastructure``), so that a raw generation-write method or
  call, an alias, a direct call of the private persistence helpers, a write
  from Infrastructure code, an issuer, or a construction of the adapter
  outside a recorder wiring cannot be reintroduced silently.

The scan reads repository-local source only, so it runs in Infrastructure CI
as checked out (Infrastructure, Core and Application ``develop``). API and Web
are not checked out there; their read-only inspection is reported review
evidence, not a test here. Test code, including the labelled test-only
raw-SQL setup (``plant_generation_records``, ``bypass``), is outside the
production scan by construction; a separate scan keeps the private issuer out
of the tests too.

The scan works on the syntax tree, so comments, docstrings and strings never
count. It is focused on the EI4-D11 generation store: ``record_generation``,
``record_attempt``, ``_record_generation`` and ``_record_attempt`` are
reserved names; similarly named operations of other stores (``record_audit``,
``record_progress``, ``_record_one``, the other adapters' ``_write``) are not
flagged. No production equity recommendation-generation composition root
exists today; the composition-root rule admits a construction of the adapter
only as an argument of the verified recorder or a read-only use case. The
boundary is not tamper-proof against privileged code, direct SQL or file
access (ADR-022 DQ9, ADR-023).
"""

from __future__ import annotations

import ast
import inspect
import pathlib
import socket
import sqlite3
from pathlib import Path
from typing import Any

import pytest
from northstar_application.ports import (
    EquityRecommendationGenerationRecordReader,
    EquityRecommendationGenerationRecordWriter,
    EquityRecommendationGenerationStore,
    UnverifiedEquityRecommendationGenerationWriteError,
)

import northstar_infrastructure
from northstar_infrastructure import persistence
from northstar_infrastructure.persistence import SQLiteEquityRecommendationGenerationStore
from northstar_infrastructure.persistence import (
    sqlite_equity_recommendation_generation as adapter_module,
)
from tests.equity_recommendation_generation_fixtures import World

_Adapter = SQLiteEquityRecommendationGenerationStore
_REQUIRED = frozenset({"load_disposition", "load_attempt", "write_generation", "write_attempt"})
_RAW_WRITES = frozenset({"record_generation", "record_attempt"})
_AUTHORIZED_WRITES = frozenset({"write_generation", "write_attempt"})
_PRIVATE_PERSISTENCE = frozenset({"_record_generation", "_record_attempt"})
_VALIDATORS = {
    "write_generation": "require_verified_equity_recommendation_generation_write",
    "write_attempt": "require_verified_equity_recommendation_attempt_write",
}
_PERSISTS = {"write_generation": "_record_generation", "write_attempt": "_record_attempt"}
_GENERATION_TYPES = frozenset(
    {
        "EquityRecommendationGenerationRecordWriter",
        "EquityRecommendationGenerationStore",
        "SQLiteEquityRecommendationGenerationStore",
    }
)
_ISSUER_NAMES = frozenset({"_issue_generation_write", "_issue_attempt_write", "_ISSUED"})
_AUTHORIZATION_MODULE = "equity_recommendation_generation_write_authorization"
_AUTHORIZATION_TYPES = frozenset(
    {"VerifiedEquityRecommendationGenerationWrite", "VerifiedEquityRecommendationAttemptWrite"}
)
_WIRING = frozenset(
    {
        "RecordEquityRecommendationGenerationUseCase",
        "VerifyStoredEquityRecommendationGenerationUseCase",
        "AuditEquityRecommendationPolicyBindingsUseCase",
    }
)
_ADAPTER_MODULE = "persistence/sqlite_equity_recommendation_generation.py"
_SWITCH_WORDS = ("allow", "raw", "unsafe", "bypass", "debug", "test", "force", "skip", "verify")


@pytest.fixture(autouse=True)
def _refuse_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("EI4-D11-W4-I tests must not open network connections.")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


# ----------------------------------------------------------------------
# The source scan
# ----------------------------------------------------------------------


def _package_root() -> pathlib.Path:
    return pathlib.Path(northstar_infrastructure.__file__).parent


def _repository_tests() -> pathlib.Path:
    return pathlib.Path(__file__).parent


def _names(node: ast.AST) -> set[str]:
    return {n.id for n in ast.walk(node) if isinstance(n, ast.Name)} | {
        n.attr for n in ast.walk(node) if isinstance(n, ast.Attribute)
    }


def _base_names(node: ast.ClassDef) -> set[str]:
    return {b.id if isinstance(b, ast.Name) else getattr(b, "attr", "") for b in node.bases}


def _called(node: ast.Call) -> str | None:
    func = node.func
    if isinstance(func, ast.Attribute):
        return func.attr
    if isinstance(func, ast.Name):
        if (
            func.id == "getattr"
            and len(node.args) >= 2
            and isinstance(node.args[1], ast.Constant)
            and isinstance(node.args[1].value, str)
        ):
            return node.args[1].value
        return func.id
    return None


class _BoundaryScan(ast.NodeVisitor):
    """Collects EI4-D11 write-boundary violations in one Infrastructure production module."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.violations: list[str] = []
        self._classes: list[ast.ClassDef] = []
        self._functions: list[str] = []
        self._parents: dict[ast.AST, ast.AST] = {}

    def scan(self, tree: ast.AST) -> list[str]:
        for parent in ast.walk(tree):
            for child in ast.iter_child_nodes(parent):
                self._parents[child] = parent
        self.visit(tree)
        return self.violations

    def _flag(self, node: ast.AST, message: str) -> None:
        self.violations.append(f"{self.path}:{getattr(node, 'lineno', '?')}: {message}")

    def _generation_class(self, node: ast.ClassDef) -> bool:
        return node.name in _GENERATION_TYPES or bool(_base_names(node) & _GENERATION_TYPES)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        if self._generation_class(node):
            for item in node.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    if item.name in _RAW_WRITES:
                        self._flag(item, f"{node.name} declares raw generation write {item.name}")
                    elif not item.name.startswith("_") and item.name not in _REQUIRED:
                        self._flag(
                            item,
                            f"{node.name} adds public method {item.name}; the generation "
                            "store exposes only the port's reads and authorized writes",
                        )
                elif isinstance(item, (ast.Assign, ast.AnnAssign)):
                    targets = item.targets if isinstance(item, ast.Assign) else [item.target]
                    for target in targets:
                        for name in _names(target):
                            if not name.startswith("_"):
                                self._flag(
                                    item,
                                    f"{node.name} exposes public attribute {name}; a public "
                                    "alias may not reach the private persistence helpers",
                                )
        self._classes.append(node)
        self.generic_visit(node)
        self._classes.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._functions.append(node.name)
        self.generic_visit(node)
        self._functions.pop()

    def _own_method(self, name: str) -> bool:
        if not self._classes or self._generation_class(self._classes[-1]):
            return False
        return any(
            isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name
            for n in self._classes[-1].body
        )

    def visit_Call(self, node: ast.Call) -> None:
        name = _called(node)
        on_self = isinstance(node.func, ast.Attribute) and (
            isinstance(node.func.value, ast.Name) and node.func.value.id == "self"
        )
        function = self._functions[-1] if self._functions else None
        if name in _RAW_WRITES and not (on_self and self._own_method(name)):
            self._flag(
                node,
                f"calls raw generation write {name}(); generation records are written only by "
                "the verified Application recorder through write_generation / write_attempt",
            )
        if name in _AUTHORIZED_WRITES:
            self._flag(node, f"calls {name}() from Infrastructure; only the recorder writes")
        if name in _PRIVATE_PERSISTENCE and not (
            self.path == _ADAPTER_MODULE and on_self and _PERSISTS.get(function or "") == name
        ):
            self._flag(
                node,
                f"calls private persistence {name}() outside its authorized write entry "
                f"({function or 'module level'})",
            )
        if (
            name == "_write"
            and self.path == _ADAPTER_MODULE
            and function not in _PRIVATE_PERSISTENCE
        ):
            self._flag(node, f"calls the transaction _write() from {function or 'module level'}")
        if name in _AUTHORIZATION_TYPES:
            self._flag(node, f"constructs {name}; only the verified recorder issues one")
        if name == "SQLiteEquityRecommendationGenerationStore" and self.path != _ADAPTER_MODULE:
            parent = self._parents.get(node)
            wired = isinstance(parent, ast.Call) and _called(parent) in _WIRING
            if not wired:
                self._flag(
                    node,
                    "constructs SQLiteEquityRecommendationGenerationStore outside a recorder "
                    "or read-only use-case wiring (composition-root rule)",
                )
        self.generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:
        if node.id in _ISSUER_NAMES:
            self._flag(node, f"references the Application's private issuer {node.id}")

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if node.attr in _ISSUER_NAMES:
            self._flag(node, f"references the Application's private issuer {node.attr}")
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        for alias in node.names:
            if alias.name in _ISSUER_NAMES:
                self._flag(node, f"imports the Application's private issuer {alias.name}")
        if (node.module or "").endswith(_AUTHORIZATION_MODULE):
            self._flag(node, "imports the authorization module directly; use the public exports")


def boundary_violations(source: str, path: str) -> list[str]:
    """The EI4-D11 violations in ``source``, a module at ``path`` (relative, with ``/``)."""
    return _BoundaryScan(path).scan(ast.parse(source))


def _production_modules() -> list[tuple[str, str]]:
    root = _package_root()
    return [
        (path.relative_to(root).as_posix(), path.read_text(encoding="utf-8"))
        for path in sorted(root.rglob("*.py"))
    ]


def _adapter_method(name: str) -> ast.FunctionDef:
    tree = ast.parse(inspect.getsource(adapter_module))
    (adapter,) = [
        n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == _Adapter.__name__
    ]
    (method,) = [n for n in adapter.body if isinstance(n, ast.FunctionDef) and n.name == name]
    return method


# ----------------------------------------------------------------------
# A. The adapter's public surface
# ----------------------------------------------------------------------


def test_a_the_adapter_exposes_exactly_the_port_operations(tmp_path: Path) -> None:
    public = {n for n in dir(_Adapter) if not n.startswith("_")}
    assert public == _REQUIRED == EquityRecommendationGenerationStore.__abstractmethods__
    for name in _RAW_WRITES:
        assert not hasattr(_Adapter, name), name
    for name in _REQUIRED:
        member = vars(_Adapter)[name]  # the adapter's own, never an inherited default
        assert inspect.isfunction(member) and member.__name__ == name, name  # no alias
        assert not getattr(member, "__isabstractmethod__", False), name
    for name in _PRIVATE_PERSISTENCE:
        assert inspect.isfunction(vars(_Adapter)[name]), name  # private, and not re-exposed
        assert all(getattr(_Adapter, p) is not vars(_Adapter)[name] for p in public)
    store = _Adapter(tmp_path / "unused.sqlite3")
    assert isinstance(store, EquityRecommendationGenerationRecordReader)
    assert isinstance(store, EquityRecommendationGenerationRecordWriter)
    assert not (tmp_path / "unused.sqlite3").exists()  # constructing opens nothing


def test_a_the_constructor_has_no_switch_and_the_module_reads_no_environment() -> None:
    assert list(inspect.signature(_Adapter).parameters) == ["database_path"]
    names = _names(ast.parse(inspect.getsource(adapter_module)))
    assert not names & {"environ", "getenv", "putenv"}
    assert not [n for n in names if any(f"{w}_write" in n.lower() for w in _SWITCH_WORDS)]
    assert not {n for n in names if n.lower() in {"allow_raw", "unsafe", "bypass", "testing"}}


def test_a_the_package_exports_the_adapter_and_nothing_private() -> None:
    assert persistence.SQLiteEquityRecommendationGenerationStore is _Adapter
    exported = set(persistence.__all__)
    assert "SQLiteEquityRecommendationGenerationStore" in exported
    forbidden = _RAW_WRITES | _PRIVATE_PERSISTENCE | _ISSUER_NAMES | {"_write"}
    assert not exported & forbidden
    assert not {n for n in exported if "plant" in n.lower() or "bypass" in n.lower()}


# ----------------------------------------------------------------------
# B. Mandatory validation, before any connection
# ----------------------------------------------------------------------


@pytest.mark.parametrize("entry", sorted(_AUTHORIZED_WRITES))
def test_b_each_write_entry_validates_first_with_no_branch_around_it(entry: str) -> None:
    method = _adapter_method(entry)
    body = method.body[1:] if ast.get_docstring(method) else method.body  # the docstring only
    first = body[0]
    assert isinstance(first, ast.Assign) and isinstance(first.value, ast.Call)
    assert ast.unparse(first.value.func) == _VALIDATORS[entry]
    assert [ast.unparse(a) for a in first.value.args] == ["authorization"]
    branches = (ast.If, ast.IfExp, ast.Try, ast.Match, ast.While, ast.For, ast.BoolOp)
    assert not [n for n in ast.walk(method) if isinstance(n, branches)], entry
    calls = [_called(n) for n in ast.walk(method) if isinstance(n, ast.Call)]
    assert calls.count(_VALIDATORS[entry]) == 1 and _PERSISTS[entry] in calls
    assert "authorization" not in _names(ast.Module(body=body[1:], type_ignores=[]))


@pytest.mark.parametrize("entry", sorted(_AUTHORIZED_WRITES))
def test_b_validation_returns_before_sqlite_is_opened(
    entry: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    world = World(tmp_path / "northstar.sqlite3")
    audit = world.proposed() if entry == "write_generation" else world.attempt_only()
    authorization = world.authorize(audit)
    events: list[str] = []
    real_validator = getattr(adapter_module, _VALIDATORS[entry])
    real_connect = adapter_module._connect

    def validate(candidate: object) -> Any:
        events.append("validate")
        return real_validator(candidate)

    def connect(database_path: Path) -> sqlite3.Connection:
        events.append("connect")
        return real_connect(database_path)

    monkeypatch.setattr(adapter_module, _VALIDATORS[entry], validate)
    monkeypatch.setattr(adapter_module, "_connect", connect)
    getattr(world.store(), entry)(authorization)
    assert events[0] == "validate" and "connect" in events
    events.clear()
    with pytest.raises(UnverifiedEquityRecommendationGenerationWriteError):
        getattr(world.store(), entry)(None)
    assert events == ["validate"]  # refused: no connection at all


# ----------------------------------------------------------------------
# C. The production source scan
# ----------------------------------------------------------------------


def test_c_infrastructure_production_source_respects_the_write_boundary() -> None:
    modules = _production_modules()
    assert any(path == _ADAPTER_MODULE for path, _ in modules)
    violations = [v for path, source in modules for v in boundary_violations(source, path)]
    assert violations == [], "\n".join(violations)


def test_c_the_caller_inventory_is_exactly_the_adapter_and_its_export() -> None:
    """Every production reference to the generation store, by module (EI4-D11 row 29)."""
    referencing = set()
    for path, source in _production_modules():
        tree = ast.parse(source)
        imported = {
            alias.name for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) for alias in n.names
        }
        defined = {n.name for n in ast.walk(tree) if isinstance(n, ast.ClassDef)}
        if "SQLiteEquityRecommendationGenerationStore" in _names(tree) | imported | defined:
            referencing.add(path)
    # The adapter itself and the package export: no composition root exists today.
    assert referencing == {_ADAPTER_MODULE, "persistence/__init__.py"}


def test_c_the_adapter_reaches_no_broker_exchange_network_or_messaging_code() -> None:
    """Row 25 (A): the generation adapter can issue nothing and place no order."""
    tree = ast.parse(inspect.getsource(adapter_module))
    modules = {n.module or "" for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)}
    modules |= {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
    for module in modules:
        parts = set(module.split("."))
        assert not parts & {"brokers", "exchanges", "fix", "messaging", "rest", "websocket"}
        assert not parts & {"socket", "http", "urllib", "requests", "httpx", "asyncio"}, module


def test_c_tests_reach_the_issuer_only_through_the_real_recorder() -> None:
    offenders = []
    for path in sorted(_repository_tests().rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imported = {
            alias.name for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) for alias in n.names
        }
        if (_names(tree) | imported) & _ISSUER_NAMES:
            offenders.append(path.name)
    assert offenders == []


_FORBIDDEN = {
    "raw-generation-method-on-the-adapter": (
        _ADAPTER_MODULE,
        "class SQLiteEquityRecommendationGenerationStore(EquityRecommendationGenerationStore):\n"
        "    def record_generation(self, disposition, attempt):\n"
        "        return self._record_generation(disposition, attempt)\n",
        "declares raw generation write record_generation",
    ),
    "raw-attempt-method-on-a-subclass": (
        "persistence/a_new_store.py",
        "class Faster(SQLiteEquityRecommendationGenerationStore):\n"
        "    def record_attempt(self, attempt):\n"
        "        return attempt\n",
        "declares raw generation write record_attempt",
    ),
    "public-alias-of-private-persistence": (
        _ADAPTER_MODULE,
        "class SQLiteEquityRecommendationGenerationStore(EquityRecommendationGenerationStore):\n"
        "    persist = _record_generation\n",
        "exposes public attribute persist",
    ),
    "raw-call-from-production": (
        "persistence/a_new_job.py",
        "def run(store, d, a):\n    return store.record_generation(d, a)\n",
        "calls raw generation write record_generation()",
    ),
    "authorized-write-from-infrastructure": (
        "persistence/a_new_job.py",
        "def run(store, token):\n    return store.write_attempt(token)\n",
        "calls write_attempt() from Infrastructure",
    ),
    "private-persistence-called-directly": (
        "persistence/a_new_job.py",
        "def run(store, d, a):\n    return store._record_generation(d, a)\n",
        "calls private persistence _record_generation()",
    ),
    "private-persistence-from-the-wrong-entry": (
        _ADAPTER_MODULE,
        "class SQLiteEquityRecommendationGenerationStore(EquityRecommendationGenerationStore):\n"
        "    def write_attempt(self, authorization):\n"
        "        verified = authorization\n"
        "        return self._record_generation(verified.disposition, verified.attempt)\n",
        "calls private persistence _record_generation() outside its authorized write entry",
    ),
    "transaction-reached-around-the-helpers": (
        _ADAPTER_MODULE,
        "class SQLiteEquityRecommendationGenerationStore(EquityRecommendationGenerationStore):\n"
        "    def load_attempt(self, identity):\n"
        "        return self._write(lambda c: c)\n",
        "calls the transaction _write() from load_attempt",
    ),
    "authorization-constructed-in-infrastructure": (
        "persistence/a_new_job.py",
        "def run(attempt):\n    return VerifiedEquityRecommendationAttemptWrite(attempt)\n",
        "constructs VerifiedEquityRecommendationAttemptWrite",
    ),
    "private-issuer-imported": (
        "persistence/a_new_job.py",
        "from northstar_application.application_services.equity_recommendation_generation_write_"
        "authorization import _issue_attempt_write\n",
        "imports the Application's private issuer _issue_attempt_write",
    ),
    "adapter-constructed-outside-a-recorder-wiring": (
        "rest/a_new_route.py",
        "def handler(path):\n    store = SQLiteEquityRecommendationGenerationStore(path)\n"
        "    return store\n",
        "composition-root rule",
    ),
}


@pytest.mark.parametrize("case", list(_FORBIDDEN))
def test_c_the_scan_detects_a_deliberately_introduced_violation(case: str) -> None:
    path, source, expected = _FORBIDDEN[case]
    violations = boundary_violations(source, path)
    assert any(expected in v for v in violations), violations
    assert all(v.startswith(f"{path}:") for v in violations)  # it names the file and line


_HARMLESS = {
    "comment-docstring-and-string": (
        "persistence/a_new_job.py",
        '"""The raw store.record_generation(d, a) was removed in W4-I."""\n'
        "# self._record_attempt(attempt) is private\n"
        "MESSAGE = 'record_attempt is not an authorized write path'\n",
    ),
    "other-stores-similarly-named-operations": (
        "persistence/sqlite_futures_trade_recommendations.py",
        "class SQLiteFuturesTradeRecommendationLifecycleStore(Store):\n"
        "    def record_audit(self, audit):\n"
        "        return self._record_one(audit)\n"
        "    def record_progress(self, progress):\n"
        "        return self._write(progress)\n",
    ),
    "a-future-recorder-wiring": (
        "rest/a_new_route.py",
        "def recorder(path, verifier, catalog):\n"
        "    return RecordEquityRecommendationGenerationUseCase(\n"
        "        SQLiteEquityRecommendationGenerationStore(path), verifier, catalog, 3\n"
        "    )\n",
    ),
    "the-adapter-own-authorized-entries": (
        _ADAPTER_MODULE,
        "class SQLiteEquityRecommendationGenerationStore(EquityRecommendationGenerationStore):\n"
        "    def write_attempt(self, authorization):\n"
        "        verified = require_verified_equity_recommendation_attempt_write(authorization)\n"
        "        return self._record_attempt(verified.attempt)\n"
        "    def _record_attempt(self, attempt):\n"
        "        return self._write(attempt)\n",
    ),
}


@pytest.mark.parametrize("case", list(_HARMLESS))
def test_c_the_scan_ignores_text_other_stores_and_supported_wiring(case: str) -> None:
    path, source = _HARMLESS[case]
    assert boundary_violations(source, path) == []
