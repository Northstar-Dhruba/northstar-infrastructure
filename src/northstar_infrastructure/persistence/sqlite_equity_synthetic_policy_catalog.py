"""SQLite store of the immutable synthetic policy catalog (EI4-D10-S2, ADR-022).

Implements the Application's EquitySyntheticPolicyCatalogStore under EI4-D10
(EI-4 Decision Register, section 10.2), its approved design decisions
EI4-D10-DQ1 to EI4-D10-DQ9 (section 11) and ADR-022. It is a policy authority
only: it holds published synthetic policy bindings, never a generation
record, never an issued recommendation and never an issuance path. It is wired
into no operation, and it calls no provider, NSE, broker or network.

One database authority
----------------------
It uses the SAME database file as the EI-2B evidence, the EI-3C timelines and
the EI-4D generation records (EI4-D10-DQ4). It never creates a database file,
and it refuses a database without the EI-2B and EI-3C tables, so the catalog
can never become a second authority. Every connection turns foreign keys on and
confirms they are on.

Records, not decisions
----------------------
Records are the Application's EquitySyntheticPolicyDefinitionBinding and
EquitySyntheticPolicySetBinding (equity_synthetic_policy_bindings). A stored
row is returned only by rebuilding its binding with the Application's
``from_stored`` -- which verifies the canonical encoding, the format and the
``eipd1`` fingerprint -- and confirming that every structured column is what
that binding produces. A stored set is returned only when its five stored
components rebuild, with it, an EquitySyntheticPolicyBundle, which re-derives
its existing ``eirs1``. Otherwise InvalidEquitySyntheticPolicyBindingError is
raised, or IncompatibleEquitySyntheticPolicyBindingError for an unsupported
format. Nothing is repaired, migrated or skipped, and this module computes no
fingerprint of its own.

Publication
-----------
``publish(bundle)`` runs ONE ``BEGIN IMMEDIATE`` transaction, which takes the
write lock before reading:

1. each of the bundle's five components, then its set, is read and rebuilt;
2. any stored binding that differs from the offered one -- in any field --
   is collected. If there is one, nothing is written, the transaction is
   rolled back, and CONFLICT is reported with every differing stored binding;
3. all six already stored identically: nothing is written, and EQUIVALENT;
4. otherwise the next publication row is inserted, then every missing
   component (an identical stored component is shared, never duplicated),
   then the set, and the transaction commits once: COMMITTED.

Failures
--------
- EquitySyntheticPolicyCatalogUnavailableError: the database could not be
  opened or read, or is not the evidence database; nothing was written.
- EquitySyntheticPolicyCatalogNotCommittedError: the transaction ended before
  COMMIT -- the write lock not obtained within five seconds, or a row refused
  by the database; nothing was written.
- InvalidEquitySyntheticPolicyBindingError (or its Incompatible subclass): a
  stored binding does not rebuild; the transaction was rolled back.
- EquitySyntheticPolicyCatalogCommitUnconfirmedError: COMMIT itself failed.
  SQLite may or may not have made the publication durable. Nothing is
  asserted, and the Application reconciles by reload.

Cleanup -- rolling back or closing a connection that may already have failed,
in ``publish`` or when opening one fails -- never replaces the outcome being
reported (L9-P, as W2 for the EI-4D-B store). After a definite outcome --
COMMITTED, EQUIVALENT or CONFLICT -- the connection is closed normally, and a
failure to close it is reported as itself.

The store never retries, sleeps or reads a clock. ``publication_sequence``
orders publications within the catalog only. No EI-4D record holds it, so it
is no evidence of when a binding existed relative to a generation record, and
this module implements no EquitySyntheticPolicyPublicationHistory.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from contextlib import closing
from pathlib import Path
from typing import Any, NoReturn

from northstar_application.application_services import (
    EQUITY_SYNTHETIC_POLICY_COMPONENT_TYPES,
    EquitySyntheticPolicyBindingIdentity,
    EquitySyntheticPolicyBundle,
    EquitySyntheticPolicyDefinitionBinding,
    EquitySyntheticPolicySetBinding,
    EquitySyntheticPolicyType,
    InvalidEquitySyntheticPolicyBindingError,
)
from northstar_application.ports import (
    EquitySyntheticPolicyCatalogCommitUnconfirmedError,
    EquitySyntheticPolicyCatalogNotCommittedError,
    EquitySyntheticPolicyCatalogOutcome,
    EquitySyntheticPolicyCatalogStore,
    EquitySyntheticPolicyCatalogUnavailableError,
    EquitySyntheticPolicyCatalogWrite,
)

from northstar_infrastructure.persistence.sqlite_equity_candle_evidence_schema import (
    initialize_equity_candle_evidence_schema,
)
from northstar_infrastructure.persistence.sqlite_equity_intraday_research_timeline_schema import (  # noqa: E501
    initialize_equity_intraday_research_timeline_schema,
)
from northstar_infrastructure.persistence.sqlite_equity_synthetic_policy_catalog_schema import (  # noqa: E501
    DEFINITIONS,
    POLICY_SETS,
    PUBLICATIONS,
    SET_COMPONENT_COLUMNS,
    initialize_equity_synthetic_policy_catalog_schema,
)

_BUSY_TIMEOUT_SECONDS = 5.0
_DECODE_ERRORS = (KeyError, TypeError, ValueError, AttributeError)
_OUTCOME = EquitySyntheticPolicyCatalogOutcome
_T = EquitySyntheticPolicyType
_Binding = EquitySyntheticPolicyDefinitionBinding | EquitySyntheticPolicySetBinding

DEFINITION_COLUMNS = (
    "namespace",
    "policy_type",
    "version_label",
    "definition_format",
    "definition_fingerprint",
    "definition_document",
    "publication_sequence",
)
SET_COLUMNS = (
    "namespace",
    "set_version_label",
    "reference_document",
    "policy_set_fingerprint",
    *(
        column
        for prefix in SET_COMPONENT_COLUMNS.values()
        for column in (f"{prefix}_type", f"{prefix}_version")
    ),
    "publication_sequence",
)
PUBLICATION_COLUMNS = (
    "publication_sequence",
    "namespace",
    "set_version_label",
    "policy_set_fingerprint",
)
_SELECT_DEFINITION = (
    f"SELECT {', '.join(DEFINITION_COLUMNS)} FROM {DEFINITIONS} "  # noqa: S608
    "WHERE namespace = ? AND policy_type = ? AND version_label = ?"
)
_SELECT_SET = (
    f"SELECT {', '.join(SET_COLUMNS)} FROM {POLICY_SETS} "  # noqa: S608
    "WHERE namespace = ? AND set_version_label = ?"
)
_INSERT_PUBLICATION = (
    f"INSERT INTO {PUBLICATIONS} ({', '.join(PUBLICATION_COLUMNS)}) "  # noqa: S608
    f"VALUES ({', '.join('?' * len(PUBLICATION_COLUMNS))})"
)
_INSERT_DEFINITION = (
    f"INSERT INTO {DEFINITIONS} ({', '.join(DEFINITION_COLUMNS)}) "  # noqa: S608
    f"VALUES ({', '.join('?' * len(DEFINITION_COLUMNS))})"
)
_INSERT_SET = (
    f"INSERT INTO {POLICY_SETS} ({', '.join(SET_COLUMNS)}) "  # noqa: S608
    f"VALUES ({', '.join('?' * len(SET_COLUMNS))})"
)
_NEXT_SEQUENCE = f"SELECT COALESCE(MAX(publication_sequence), 0) + 1 FROM {PUBLICATIONS}"  # noqa: S608


def _invalid(message: str) -> NoReturn:
    raise InvalidEquitySyntheticPolicyBindingError(message)


def _decoded[T](build: Callable[[], T], subject: str) -> T:
    """Rebuild a binding from stored values; any failure is an invalid stored binding."""
    try:
        return build()
    except InvalidEquitySyntheticPolicyBindingError:
        raise
    except _DECODE_ERRORS as exc:
        raise InvalidEquitySyntheticPolicyBindingError(
            f"{subject} does not rebuild: {exc}"
        ) from exc


# ---------------------------------------------------------------------------
# Rows
# ---------------------------------------------------------------------------


def definition_row(
    binding: EquitySyntheticPolicyDefinitionBinding, publication_sequence: int
) -> tuple[object, ...]:
    """Return the stored row of a component binding."""
    return (
        binding.namespace,
        binding.policy_type.value,
        binding.version.value,
        binding.document()["format"],
        binding.fingerprint,
        binding.canonical_json(),
        publication_sequence,
    )


def set_row(
    binding: EquitySyntheticPolicySetBinding, publication_sequence: int
) -> tuple[object, ...]:
    """Return the stored row of a POLICY_SET binding."""
    return (
        binding.namespace,
        binding.version.value,
        binding.reference_json(),
        binding.policy_set_fingerprint,
        *(
            value
            for policy_type in EQUITY_SYNTHETIC_POLICY_COMPONENT_TYPES
            for value in (
                policy_type.value,
                binding.component_identity(policy_type).version.value,
            )
        ),
        publication_sequence,
    )


def _stored_definition(
    row: Any, identity: EquitySyntheticPolicyBindingIdentity
) -> EquitySyntheticPolicyDefinitionBinding:
    """Rebuild the stored component binding; every column must be what it produces."""
    stored = tuple(row)

    def build() -> EquitySyntheticPolicyDefinitionBinding:
        binding = EquitySyntheticPolicyDefinitionBinding.from_stored(stored[5], stored[4])
        if binding.identity != identity:
            _invalid("The stored binding is not the binding of its identity.")
        if definition_row(binding, stored[6])[:6] != stored[:6]:
            _invalid("The stored binding has columns that disagree with its document.")
        return binding

    return _decoded(build, "The stored policy binding")


def _stored_set(
    connection: sqlite3.Connection, row: Any, identity: EquitySyntheticPolicyBindingIdentity
) -> EquitySyntheticPolicySetBinding:
    """Rebuild the stored set; its five stored components must re-derive its eirs1."""
    stored = tuple(row)

    def build() -> EquitySyntheticPolicySetBinding:
        binding = EquitySyntheticPolicySetBinding.from_stored(stored[0], stored[2], stored[3])
        if binding.identity != identity:
            _invalid("The stored policy set is not the set of its identity.")
        if set_row(binding, stored[-1])[:-1] != stored[:-1]:
            _invalid("The stored policy set has columns that disagree with its reference.")
        components = []
        for policy_type in EQUITY_SYNTHETIC_POLICY_COMPONENT_TYPES:
            component_identity = binding.component_identity(policy_type)
            component_row = connection.execute(
                _SELECT_DEFINITION, _key(component_identity)
            ).fetchone()
            if component_row is None:
                _invalid(f"The stored policy set names a missing {policy_type.value} binding.")
            components.append(_stored_definition(component_row, component_identity))
        EquitySyntheticPolicyBundle(tuple(components), binding)  # re-derives its eirs1
        return binding

    return _decoded(build, "The stored policy set")


def _key(identity: EquitySyntheticPolicyBindingIdentity) -> tuple[str, str, str]:
    return (identity.namespace, identity.policy_type.value, identity.version.value)


def _load(connection: sqlite3.Connection, binding: _Binding) -> _Binding | None:
    identity = binding.identity
    if isinstance(binding, EquitySyntheticPolicySetBinding):
        row = connection.execute(
            _SELECT_SET, (identity.namespace, identity.version.value)
        ).fetchone()
        return None if row is None else _stored_set(connection, row, identity)
    row = connection.execute(_SELECT_DEFINITION, _key(identity)).fetchone()
    return None if row is None else _stored_definition(row, identity)


# ---------------------------------------------------------------------------
# Connections and transactions
# ---------------------------------------------------------------------------


def _unavailable(exc: BaseException) -> EquitySyntheticPolicyCatalogUnavailableError:
    return EquitySyntheticPolicyCatalogUnavailableError(
        f"The policy catalog is unavailable; nothing was written: {exc}"
    )


def _connect(database_path: Path) -> sqlite3.Connection:
    """Open the existing evidence database; never create one.

    Foreign keys are turned on and confirmed. The catalog schema is added when
    absent -- only beside the EI-2B and EI-3C tables -- and the EI-2B and
    EI-3C initializers restore only their own missing indexes or triggers.
    """
    try:
        connection = sqlite3.connect(
            f"{database_path.resolve().as_uri()}?mode=rw",
            timeout=_BUSY_TIMEOUT_SECONDS,
            isolation_level=None,
            uri=True,
        )
    except sqlite3.Error as exc:
        raise _unavailable(exc) from exc
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        if connection.execute("PRAGMA foreign_keys").fetchone() != (1,):
            raise sqlite3.OperationalError("foreign keys could not be enforced")
        initialize_equity_synthetic_policy_catalog_schema(connection)
        initialize_equity_candle_evidence_schema(connection)
        initialize_equity_intraday_research_timeline_schema(connection)
    except sqlite3.Error as exc:
        _discard(connection)
        raise _unavailable(exc) from exc
    return connection


def _commit(connection: sqlite3.Connection) -> None:
    """Commit the open transaction: the one boundary whose failure is unconfirmed."""
    connection.commit()


def _insert_publication(connection: sqlite3.Connection, values: tuple[object, ...]) -> None:
    connection.execute(_INSERT_PUBLICATION, values)


def _insert_definition(connection: sqlite3.Connection, values: tuple[object, ...]) -> None:
    connection.execute(_INSERT_DEFINITION, values)


def _insert_set(connection: sqlite3.Connection, values: tuple[object, ...]) -> None:
    connection.execute(_INSERT_SET, values)


def _rollback(connection: sqlite3.Connection) -> None:
    """Roll back an open transaction; never mask the failure that led here.

    A connection that already failed or closed -- for example around COMMIT --
    cannot report its state; closing it rolls back whatever SQLite still holds.
    """
    try:
        if connection.in_transaction:
            connection.rollback()
    except sqlite3.Error:
        pass


def _close(connection: sqlite3.Connection) -> None:
    connection.close()


def _discard(connection: sqlite3.Connection) -> None:
    """Close a connection as a failure propagates; a close failure never replaces it."""
    try:
        _close(connection)
    except sqlite3.Error:
        pass


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class SQLiteEquitySyntheticPolicyCatalogStore(EquitySyntheticPolicyCatalogStore):
    """Insert-only SQLite catalog of published synthetic policy bindings.

    ``database_path`` must be the existing EI-2B and EI-3C evidence database:
    there is no second database setting.
    """

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = Path(database_path)

    # -- reads ----------------------------------------------------------------

    def load_definition(
        self, identity: EquitySyntheticPolicyBindingIdentity
    ) -> EquitySyntheticPolicyDefinitionBinding | None:
        """Return the component binding published under ``identity``, or None."""
        if not isinstance(identity, EquitySyntheticPolicyBindingIdentity):
            raise TypeError("identity must be an EquitySyntheticPolicyBindingIdentity.")
        if identity.policy_type is _T.POLICY_SET:
            raise ValueError("A POLICY_SET identity is loaded with load_policy_set.")

        def work(connection: sqlite3.Connection) -> Any:
            row = connection.execute(_SELECT_DEFINITION, _key(identity)).fetchone()
            return None if row is None else _stored_definition(row, identity)

        return self._read(work)  # type: ignore[no-any-return]

    def load_policy_set(
        self, identity: EquitySyntheticPolicyBindingIdentity
    ) -> EquitySyntheticPolicySetBinding | None:
        """Return the POLICY_SET binding published under ``identity``, verified, or None."""
        if not isinstance(identity, EquitySyntheticPolicyBindingIdentity):
            raise TypeError("identity must be an EquitySyntheticPolicyBindingIdentity.")
        if identity.policy_type is not _T.POLICY_SET:
            raise ValueError("Only a POLICY_SET identity is loaded with load_policy_set.")

        def work(connection: sqlite3.Connection) -> Any:
            row = connection.execute(
                _SELECT_SET, (identity.namespace, identity.version.value)
            ).fetchone()
            return None if row is None else _stored_set(connection, row, identity)

        return self._read(work)  # type: ignore[no-any-return]

    def _read(self, work: Callable[[sqlite3.Connection], Any]) -> Any:
        try:
            with closing(_connect(self._database_path)) as connection:
                return work(connection)
        except sqlite3.Error as exc:
            raise _unavailable(exc) from exc

    # -- writes ---------------------------------------------------------------

    def publish(self, bundle: EquitySyntheticPolicyBundle) -> EquitySyntheticPolicyCatalogWrite:
        """Atomically publish the whole ``bundle``: all six bindings or none."""
        if not isinstance(bundle, EquitySyntheticPolicyBundle):
            raise TypeError("bundle must be an EquitySyntheticPolicyBundle.")
        connection = _connect(self._database_path)
        try:
            written = self._publish_in(connection, bundle)
        except BaseException:
            _discard(connection)  # a close failure never replaces the failure reported
            raise
        _close(connection)  # after a definite outcome, a close failure is reported as itself
        return written

    @staticmethod
    def _publish_in(
        connection: sqlite3.Connection, bundle: EquitySyntheticPolicyBundle
    ) -> EquitySyntheticPolicyCatalogWrite:
        """The publication transaction on ``connection``, which the caller closes."""
        offered: tuple[_Binding, ...] = (*bundle.components, bundle.policy_set)
        try:
            connection.execute("BEGIN IMMEDIATE")
            stored = [_load(connection, binding) for binding in offered]
            conflicts = tuple(
                found
                for found, binding in zip(stored, offered, strict=True)
                if found is not None and found != binding
            )
            if conflicts:
                _rollback(connection)  # nothing was written
                return EquitySyntheticPolicyCatalogWrite(_OUTCOME.CONFLICT, conflicts)
            if all(found is not None for found in stored):
                _rollback(connection)  # nothing to write
                return EquitySyntheticPolicyCatalogWrite(_OUTCOME.EQUIVALENT, ())
            sequence = connection.execute(_NEXT_SEQUENCE).fetchone()[0]
            policy_set = bundle.policy_set
            _insert_publication(
                connection,
                (
                    sequence,
                    policy_set.namespace,
                    policy_set.version.value,
                    policy_set.policy_set_fingerprint,
                ),
            )
            for found, component in zip(stored, bundle.components, strict=False):
                if found is None:
                    _insert_definition(connection, definition_row(component, sequence))
            _insert_set(connection, set_row(policy_set, sequence))
        except sqlite3.Error as exc:
            _rollback(connection)
            raise EquitySyntheticPolicyCatalogNotCommittedError(
                f"The policy catalog refused the publication; nothing was written: {exc}"
            ) from exc
        except BaseException:
            _rollback(connection)
            raise
        try:
            _commit(connection)
        except Exception as exc:
            _rollback(connection)
            raise EquitySyntheticPolicyCatalogCommitUnconfirmedError(
                f"COMMIT failed; whether the publication is durable is unknown: {exc}"
            ) from exc
        return EquitySyntheticPolicyCatalogWrite(_OUTCOME.COMMITTED, ())
