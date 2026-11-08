"""Schema for the immutable synthetic policy catalog (EI4-D10-S2, ADR-022).

Three insert-only tables, under EI4-D10 (EI-4 Decision Register, section 10.2)
and its approved design decisions EI4-D10-DQ1 to EI4-D10-DQ9 (section 11), in
the SAME SQLite file as the EI-2B evidence, the EI-3C timelines and the EI-4D
generation records (EI4-D10-DQ4). The schema is additive: it creates only its
own tables and triggers, and it refuses a database that does not already hold
the EI-2B and EI-3C tables, so the catalog can never become a second
database authority.

- ``equity_synthetic_policy_publications`` holds one row per committed
  publication: its ``publication_sequence`` and the policy set it published.
  The sequence is a monotone recording order, 1, 2, 3, ..., enforced by the
  database. It is not a clock and holds no time.
- ``equity_synthetic_policy_definitions`` holds one component binding per
  ``(namespace, policy_type, version_label)``, its PRIMARY KEY: the exact
  canonical ``northstar.equity-synthetic-policy-definition/1`` document, its
  ``eipd1`` fingerprint, and the publication that inserted it.
- ``equity_synthetic_policy_sets`` holds one POLICY_SET binding per
  ``(namespace, set_version_label)``, its PRIMARY KEY: the exact canonical
  policy-set reference document, the existing ``eirs1`` fingerprint, the five
  component labels it names, and its publication.

Constraints
-----------
- The namespace is the approved one (EI4-D10-DQ1); a component's type is one
  of the five component types (EI4-D10-DQ2). A label is ``synthetic-``
  prefixed and carries no surrounding whitespace; it is stored exactly, never
  folded or trimmed.
- CHECK constraints tie every structured column to the document it comes
  from, so a column can never disagree with its document.
- Real foreign keys: every binding to its publication, and a set to each of
  its five component bindings, by ``(namespace, policy_type, version_label)``.
  A constant type column per component makes those keys exact. A set
  therefore cannot exist without the five components it names, under the
  types it names them as.
- Triggers keep the sequence monotone; insert a binding only in the newest
  publication, which must name the set it holds; and refuse every UPDATE and
  DELETE, and an INSERT that would replace a row.

Not expressible here, and the Application's (ADR-022, section 4): the SHA-256
fingerprints, which EquitySyntheticPolicyDefinitionBinding re-derives on every
construction, and whether a set's ``eirs1`` is the fingerprint of its
components, which EquitySyntheticPolicyBundle verifies. A foreign key proves
that a row exists, never that its content is canonical.
"""

from __future__ import annotations

import sqlite3

from northstar_application.application_services import EQUITY_SYNTHETIC_POLICY_NAMESPACE

PUBLICATIONS = "equity_synthetic_policy_publications"
DEFINITIONS = "equity_synthetic_policy_definitions"
POLICY_SETS = "equity_synthetic_policy_sets"
CATALOG_TABLES = (PUBLICATIONS, DEFINITIONS, POLICY_SETS)
REQUIRED_EVIDENCE_TABLES = (
    "equity_candle_evidence_snapshots",
    "equity_intraday_research_timelines",
    "equity_intraday_research_timeline_entries",
)

# Component type -> the policy-set reference field and the column prefix in the set table.
SET_COMPONENT_COLUMNS = {
    "GENERATION_POLICY": "generation_policy",
    "RISK_POLICY": "risk_policy",
    "COST_SCHEDULE": "cost_schedule",
    "PRICE_INCREMENT": "price_increment",
    "TIMING_POLICY": "timing_policy",
}

_NAMESPACE = EQUITY_SYNTHETIC_POLICY_NAMESPACE
_TYPES = "(" + ", ".join(f"'{t}'" for t in SET_COMPONENT_COLUMNS) + ")"
_HEX64 = "[0-9a-f]" * 64
_WHITESPACE = "char(32, 9, 10, 11, 12, 13)"


def _label(column: str) -> str:
    """A ``synthetic-`` label with no surrounding whitespace, stored exactly."""
    return (
        f"typeof({column}) = 'text' AND {column} GLOB 'synthetic-?*' "
        f"AND {column} = trim({column}, {_WHITESPACE})"
    )


EQUITY_SYNTHETIC_POLICY_PUBLICATION_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS {PUBLICATIONS} (
    publication_sequence INTEGER NOT NULL PRIMARY KEY
        CHECK (typeof(publication_sequence) = 'integer' AND publication_sequence >= 1),
    namespace TEXT NOT NULL CHECK (namespace = '{_NAMESPACE}'),
    set_version_label TEXT NOT NULL CHECK ({_label("set_version_label")}),
    policy_set_fingerprint TEXT NOT NULL
        CHECK (policy_set_fingerprint GLOB 'eirs1-sha256:{_HEX64}')
)
"""

EQUITY_SYNTHETIC_POLICY_DEFINITION_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS {DEFINITIONS} (
    namespace TEXT NOT NULL CHECK (namespace = '{_NAMESPACE}'),
    policy_type TEXT NOT NULL CHECK (policy_type IN {_TYPES}),
    version_label TEXT NOT NULL CHECK ({_label("version_label")}),
    definition_format TEXT NOT NULL CHECK (length(definition_format) > 0),
    definition_fingerprint TEXT NOT NULL UNIQUE
        CHECK (definition_fingerprint GLOB 'eipd1-sha256:{_HEX64}'),
    definition_document TEXT NOT NULL CHECK (json_valid(definition_document)),
    publication_sequence INTEGER NOT NULL,
    PRIMARY KEY (namespace, policy_type, version_label),
    FOREIGN KEY (publication_sequence) REFERENCES {PUBLICATIONS} (publication_sequence),
    CHECK (
        json_extract(definition_document, '$.format') IS definition_format
        AND json_extract(definition_document, '$.namespace') IS namespace
        AND json_extract(definition_document, '$.policy_type') IS policy_type
        AND json_extract(definition_document, '$.version') IS version_label
        AND json_extract(definition_document, '$.definition.version') IS version_label
    )
)
"""


def _set_component_columns() -> str:
    lines = []
    for policy_type, prefix in SET_COMPONENT_COLUMNS.items():
        lines.append(
            f"    {prefix}_type TEXT NOT NULL CHECK ({prefix}_type = '{policy_type}'),\n"
            f"    {prefix}_version TEXT NOT NULL\n"
            f"        CHECK ({prefix}_version IS "
            f"json_extract(reference_document, '$.{prefix}_version')),"
        )
    return "\n".join(lines)


def _set_component_keys() -> str:
    return ",\n".join(
        f"    FOREIGN KEY (namespace, {prefix}_type, {prefix}_version)\n"
        f"        REFERENCES {DEFINITIONS} (namespace, policy_type, version_label)"
        for prefix in SET_COMPONENT_COLUMNS.values()
    )


EQUITY_SYNTHETIC_POLICY_SET_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS {POLICY_SETS} (
    namespace TEXT NOT NULL CHECK (namespace = '{_NAMESPACE}'),
    set_version_label TEXT NOT NULL CHECK ({_label("set_version_label")}),
    reference_document TEXT NOT NULL
        CHECK (json_valid(reference_document)
               AND json_extract(reference_document, '$.version') IS set_version_label),
    policy_set_fingerprint TEXT NOT NULL UNIQUE
        CHECK (policy_set_fingerprint GLOB 'eirs1-sha256:{_HEX64}'),
{_set_component_columns()}
    publication_sequence INTEGER NOT NULL UNIQUE,
    PRIMARY KEY (namespace, set_version_label),
    FOREIGN KEY (publication_sequence) REFERENCES {PUBLICATIONS} (publication_sequence),
{_set_component_keys()}
)
"""


def _refuse(name: str, event: str, table: str, message: str, when: str = "") -> str:
    condition = f"\n{when}" if when else ""
    return (
        f"CREATE TRIGGER IF NOT EXISTS {name}\n{event} ON {table}{condition}\n"
        f"BEGIN SELECT RAISE(ABORT, '{message}'); END"
    )


_NEWEST = f"(SELECT MAX(publication_sequence) FROM {PUBLICATIONS})"

EQUITY_SYNTHETIC_POLICY_CATALOG_TRIGGERS = (
    *(
        _refuse(f"{table}_no_{verb.lower()}", f"BEFORE {verb}", table, f"{table} is insert-only")
        for table in CATALOG_TABLES
        for verb in ("UPDATE", "DELETE")
    ),
    _refuse(
        f"{PUBLICATIONS}_no_replace",
        "BEFORE INSERT",
        PUBLICATIONS,
        f"{PUBLICATIONS} is insert-only",
        f"WHEN EXISTS (SELECT 1 FROM {PUBLICATIONS} AS p "
        "WHERE p.publication_sequence = NEW.publication_sequence)",
    ),
    _refuse(
        f"{DEFINITIONS}_no_replace",
        "BEFORE INSERT",
        DEFINITIONS,
        f"{DEFINITIONS} is insert-only",
        f"WHEN EXISTS (SELECT 1 FROM {DEFINITIONS} AS d "
        "WHERE (d.namespace = NEW.namespace AND d.policy_type = NEW.policy_type "
        "AND d.version_label = NEW.version_label) "
        "OR d.definition_fingerprint = NEW.definition_fingerprint)",
    ),
    _refuse(
        f"{POLICY_SETS}_no_replace",
        "BEFORE INSERT",
        POLICY_SETS,
        f"{POLICY_SETS} is insert-only",
        f"WHEN EXISTS (SELECT 1 FROM {POLICY_SETS} AS s "
        "WHERE (s.namespace = NEW.namespace AND s.set_version_label = NEW.set_version_label) "
        "OR s.policy_set_fingerprint = NEW.policy_set_fingerprint "
        "OR s.publication_sequence = NEW.publication_sequence)",
    ),
    _refuse(
        f"{PUBLICATIONS}_next_sequence",
        "BEFORE INSERT",
        PUBLICATIONS,
        "a publication takes the next publication sequence",
        f"WHEN NEW.publication_sequence IS NOT "
        f"(SELECT COALESCE(MAX(publication_sequence), 0) + 1 FROM {PUBLICATIONS})",
    ),
    _refuse(
        f"{DEFINITIONS}_in_newest_publication",
        "BEFORE INSERT",
        DEFINITIONS,
        "a binding is inserted only by the newest publication",
        f"WHEN NEW.publication_sequence IS NOT {_NEWEST}",
    ),
    _refuse(
        f"{POLICY_SETS}_matches_publication",
        "BEFORE INSERT",
        POLICY_SETS,
        "a policy set is inserted only by the newest publication, which names it",
        f"WHEN NEW.publication_sequence IS NOT {_NEWEST} "
        f"OR NOT EXISTS (SELECT 1 FROM {PUBLICATIONS} AS p "
        "WHERE p.publication_sequence = NEW.publication_sequence "
        "AND p.namespace = NEW.namespace AND p.set_version_label = NEW.set_version_label "
        "AND p.policy_set_fingerprint = NEW.policy_set_fingerprint)",
    ),
)


def _require_evidence_tables(connection: sqlite3.Connection) -> None:
    present = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN (?, ?, ?)",
            REQUIRED_EVIDENCE_TABLES,
        )
    }
    missing = [name for name in REQUIRED_EVIDENCE_TABLES if name not in present]
    if missing:
        raise sqlite3.OperationalError(
            "This database holds no EI-2B and EI-3C evidence ("
            + ", ".join(missing)
            + " absent); the policy catalog is kept only in the single evidence database "
            "(EI4-D10-DQ4), and no evidence table is created here."
        )


def initialize_equity_synthetic_policy_catalog_schema(connection: sqlite3.Connection) -> None:
    """Create the catalog tables and triggers when they are absent.

    The EI-2B snapshot and EI-3C timeline tables must already exist in the
    same database; otherwise sqlite3.OperationalError is raised and nothing is
    created. Existing rows, tables, indexes and triggers -- the catalog's and
    every other -- are never altered, and nothing is backfilled.
    """
    _require_evidence_tables(connection)
    for statement in (
        EQUITY_SYNTHETIC_POLICY_PUBLICATION_SCHEMA,
        EQUITY_SYNTHETIC_POLICY_DEFINITION_SCHEMA,
        EQUITY_SYNTHETIC_POLICY_SET_SCHEMA,
        *EQUITY_SYNTHETIC_POLICY_CATALOG_TRIGGERS,
    ):
        connection.execute(statement)
    connection.commit()
