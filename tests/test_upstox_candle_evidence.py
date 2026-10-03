"""Tests for the Upstox daily-candle finality evidence collector and its log.

Every HTTP exchange is faked with the native daily adapter tests' master and
candle shapes. The token is a placeholder. Nothing here touches the network or
any Northstar database, and nothing concludes that a candle is final.
"""

from __future__ import annotations

import ast
import json
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from urllib.error import URLError

import pytest

from northstar_infrastructure.market_data import (
    MalformedUpstoxCandleEvidenceError,
    TruncatedUpstoxCandleEvidenceError,
    UpstoxAccessBlockedError,
    UpstoxAuthenticationError,
    UpstoxCandleEvidenceError,
    UpstoxCandleEvidenceLog,
    UpstoxDailyCandleEvidenceCollector,
    UpstoxInstrumentResolutionError,
    UpstoxInvalidInstrumentKeyError,
    UpstoxMarketDataSourceError,
    UpstoxProviderUnavailableError,
)
from northstar_infrastructure.market_data import upstox_candle_evidence as evidence_module
from tests.test_upstox_futures_native_daily_market_data import (
    _LOT,
    _MON,
    _NIFTY_NOV,
    _NIFTY_OCT,
    _NOV_KEY,
    _OCT_KEY,
    _THU,
    _TOKEN,
    FakeFetch,
    _candle,
    _error_body,
    _gzip_json,
    _http_error,
    _master_records,
    _success,
)

_SCHEMA = "northstar.upstox-daily-candle-observation/1"
_IST = timezone(timedelta(hours=5, minutes=30))


class Clock:
    """Hands out the given instants in order; records how often it was read."""

    def __init__(self, *instants: datetime) -> None:
        self.instants = list(instants)
        self.reads = 0

    def __call__(self) -> datetime:
        self.reads += 1
        return self.instants.pop(0)


def _at(hour: int, minute: int = 0, second: int = 0, micro: int = 0) -> datetime:
    return datetime(2026, 10, 5, hour, minute, second, micro, tzinfo=UTC)


def _collector(fetch: FakeFetch, clock: Clock) -> UpstoxDailyCandleEvidenceCollector:
    return UpstoxDailyCandleEvidenceCollector(_TOKEN, clock=clock, fetch=fetch)


_DEFAULT_INSTANTS = (
    datetime(2026, 10, 5, 10, 45, tzinfo=UTC),
    datetime(2026, 10, 5, 10, 45, 1, tzinfo=UTC),
)


def _observe(
    candles: list | None = None,
    *,
    day: date = _MON,
    contract=_NIFTY_OCT,
    instants=_DEFAULT_INSTANTS,
    fetch: FakeFetch | None = None,
):
    fetch = fetch or FakeFetch(candles=_success([_candle(day)] if candles is None else candles))
    return _collector(fetch, Clock(*instants)).observe(contract, day)


def _record(observation) -> dict:
    return json.loads(observation.to_json_line())


# ---------------------------------------------------------------------------
# One observation
# ---------------------------------------------------------------------------


def test_the_first_observation_records_the_exact_candle() -> None:
    body = (
        b'{"status": "success", "data": {"candles": [["2026-10-05T00:00:00+05:30", '
        b"25100, 25250.75, 25020, 25180.50, 273000, 13500000]]}}"
    )
    record = _record(
        _observe(
            fetch=FakeFetch(candles=body),
            instants=(_at(10, 45, 0, 123456), _at(10, 45, 1, 2000)),
        )
    )

    assert record == {
        "schema": _SCHEMA,
        "collector": "northstar-infrastructure/0.1 upstox-candle-evidence/1",
        "provider": "upstox",
        "requested_at": "2026-10-05T10:45:00.123456Z",
        "received_at": "2026-10-05T10:45:01.002Z",
        "contract": {"product": "NIFTY", "exchange": "NSE", "expiration": "2026-10-27"},
        "trading_date": "2026-10-05",
        "instrument_key": _OCT_KEY,
        "lot_size": "65",
        "request": {"interval": "days/1", "from": "2026-10-05", "to": "2026-10-05"},
        "candle": {
            "provider_timestamp": "2026-10-05T00:00:00+05:30",
            "open": "25100",
            "high": "25250.75",
            "low": "25020",
            "close": "25180.50",
            "volume": str(4200 * _LOT),
            "open_interest": "13500000",
        },
        "volume_contracts": "4200",
        "volume_contracts_note": None,
    }


def test_decimal_text_is_preserved_from_the_wire() -> None:
    body = (
        b'{"status": "success", "data": {"candles": [["2026-10-05T00:00:00+05:30", '
        b"25010.50, 25100.00, 24990.05, 25050.10, 273000, 13500000.0]]}}"
    )

    candle = _record(_observe(fetch=FakeFetch(candles=body)))["candle"]

    assert (candle["open"], candle["high"], candle["low"], candle["close"]) == (
        "25010.50",
        "25100.00",
        "24990.05",
        "25050.10",
    )
    assert candle["open_interest"] == "13500000.0"


def test_non_finite_numbers_are_refused_by_the_existing_decoder() -> None:
    body = (
        b'{"status": "success", "data": {"candles": '
        b'[["2026-10-05T00:00:00+05:30", NaN, 1, 1, 1, 65, 0]]}}'
    )

    with pytest.raises(UpstoxProviderUnavailableError, match="undecodable"):
        _observe(fetch=FakeFetch(candles=body))


def test_identity_is_the_contract_and_the_key_is_only_provider_metadata() -> None:
    observation = _observe(day=_MON, contract=_NIFTY_NOV)
    record = _record(observation)

    assert record["contract"] == {
        "product": "NIFTY",
        "exchange": "NSE",
        "expiration": "2026-11-24",
    }
    assert record["trading_date"] == "2026-10-05"
    assert observation.contract == _NIFTY_NOV
    assert record["instrument_key"] == _NOV_KEY and record["lot_size"] == str(_LOT)
    assert "url" not in json.dumps(record).lower()
    assert "historical-candle" not in json.dumps(record)


def test_raw_and_normalised_volume() -> None:
    record = _record(_observe([_candle(_MON, volume=1234 * _LOT)]))

    assert record["candle"]["volume"] == str(1234 * _LOT)
    assert (record["volume_contracts"], record["volume_contracts_note"]) == ("1234", None)


@pytest.mark.parametrize(
    ("volume", "note"),
    [
        (
            1234 * _LOT + 7,
            f"raw volume {1234 * _LOT + 7} is not a whole number of contracts at lot size 65",
        ),
        (130.5, "raw volume 130.5 is not a whole number of underlying units"),
        (-65, "raw volume -65 is negative"),
    ],
    ids=["not-whole-contracts", "fractional", "negative"],
)
def test_unconvertible_volume_is_kept_with_a_note(volume, note: str) -> None:
    record = _record(_observe([_candle(_MON, volume=volume)]))

    assert record["candle"]["volume"] == str(volume)
    assert record["volume_contracts"] is None
    assert record["volume_contracts_note"] == note


def test_open_interest_is_kept_and_may_be_absent() -> None:
    assert (
        _record(_observe([_candle(_MON, open_interest=987654)]))["candle"]["open_interest"]
        == "987654"
    )
    assert _record(_observe([_candle(_MON, open_interest=None)]))["candle"]["open_interest"] is None


def test_the_provider_timestamp_is_kept_verbatim() -> None:
    record = _record(_observe([["2026-10-05T00:00:00+05:30", 1, 2, 1, 2, 65, 0]]))

    assert record["candle"]["provider_timestamp"] == "2026-10-05T00:00:00+05:30"


def test_a_successful_response_without_a_candle_is_evidence() -> None:
    record = _record(_observe([]))

    assert record["candle"] is None
    assert record["volume_contracts"] is None and record["volume_contracts_note"] is None
    assert record["instrument_key"] == _OCT_KEY


@pytest.mark.parametrize(
    "candles",
    [[_candle(_MON), _candle(_THU)], [_candle(_THU)]],
    ids=["two-candles", "another-date"],
)
def test_more_than_one_or_another_dates_candle_is_refused(candles: list) -> None:
    with pytest.raises(UpstoxCandleEvidenceError, match="nothing was recorded"):
        _observe(candles)


def test_a_malformed_candle_is_refused() -> None:
    with pytest.raises(UpstoxCandleEvidenceError, match="malformed"):
        _observe([["2026-10-05T00:00:00+05:30", 1, 2, 3]])


# ---------------------------------------------------------------------------
# Clock
# ---------------------------------------------------------------------------


def test_the_injected_clock_is_read_around_the_request_only() -> None:
    clock = Clock(_at(10, 0), _at(10, 0, 2))
    fetch = FakeFetch(candles=_success([_candle(_MON)]))
    order: list[str] = []
    original = fetch.__call__

    def tracking(url, headers, timeout):
        order.append(f"fetch:{'master' if 'assets' in url else 'candle'}@{clock.reads}")
        return original(url, headers, timeout)

    record = _record(
        UpstoxDailyCandleEvidenceCollector(_TOKEN, clock=clock, fetch=tracking).observe(
            _NIFTY_OCT, _MON
        )
    )

    # Master resolution first, then requested_at, the candle request, received_at.
    assert order == ["fetch:master@0", "fetch:candle@1"]
    assert clock.reads == 2
    assert (record["requested_at"], record["received_at"]) == (
        "2026-10-05T10:00:00Z",
        "2026-10-05T10:00:02Z",
    )


def test_offset_instants_are_recorded_in_utc() -> None:
    record = _record(
        _observe(
            instants=(
                datetime(2026, 10, 5, 16, 15, tzinfo=_IST),
                datetime(2026, 10, 5, 16, 15, 1, tzinfo=_IST),
            )
        )
    )

    assert record["requested_at"] == "2026-10-05T10:45:00Z"


@pytest.mark.parametrize("which", ["requested_at", "received_at"])
def test_a_naive_clock_is_refused(which: str) -> None:
    naive = datetime(2026, 10, 5, 10, 45)
    instants = (naive, _at(10, 46)) if which == "requested_at" else (_at(10, 45), naive)

    with pytest.raises(ValueError, match=f"timezone-aware datetime for {which}"):
        _observe(instants=instants)


def test_the_clock_is_required() -> None:
    with pytest.raises(TypeError):
        UpstoxDailyCandleEvidenceCollector(_TOKEN, fetch=FakeFetch())  # type: ignore[call-arg]


def test_no_wall_clock_is_read_in_the_module() -> None:
    source = Path(evidence_module.__file__).read_text(encoding="utf-8")
    calls = {
        f"{node.func.value.id}.{node.func.attr}"
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
    }
    assert not {"datetime.now", "datetime.utcnow", "datetime.today", "time.time"} & calls


# ---------------------------------------------------------------------------
# Provider failures: existing classification, nothing recorded
# ---------------------------------------------------------------------------

_CANDLE_URL = "https://api.upstox.com/v3/historical-candle/x"


@pytest.mark.parametrize(
    ("fetch", "error"),
    [
        (
            FakeFetch(candle_error=_http_error(_CANDLE_URL, 403, b"error code: 1010")),
            UpstoxAccessBlockedError,
        ),
        (
            FakeFetch(
                candle_error=_http_error(
                    _CANDLE_URL, 400, _error_body("UDAPI100011", "Invalid Instrument key")
                )
            ),
            UpstoxInvalidInstrumentKeyError,
        ),
        (
            FakeFetch(candle_error=_http_error(_CANDLE_URL, 401, _error_body("UDAPI100050", "x"))),
            UpstoxAuthenticationError,
        ),
        (FakeFetch(candle_error=_http_error(_CANDLE_URL, 503)), UpstoxProviderUnavailableError),
        (FakeFetch(candle_error=URLError("down")), UpstoxProviderUnavailableError),
        (FakeFetch(master=_gzip_json([])), UpstoxInstrumentResolutionError),
        (FakeFetch(candles=b'{"status": "error"}'), UpstoxMarketDataSourceError),
    ],
    ids=[
        "cloudflare",
        "invalid-instrument",
        "authentication",
        "unavailable",
        "transport",
        "expired-contract",
        "no-success",
    ],
)
def test_provider_failures_keep_the_existing_classification(fetch, error) -> None:
    clock = Clock(_at(10, 45), _at(10, 46))

    with pytest.raises(error) as raised:
        _collector(fetch, clock).observe(_NIFTY_OCT, _MON)
    assert _TOKEN not in str(raised.value)


# ---------------------------------------------------------------------------
# The append-only log
# ---------------------------------------------------------------------------


def test_identical_observations_are_all_appended(tmp_path: Path) -> None:
    log = UpstoxCandleEvidenceLog(tmp_path / "evidence.jsonl")

    log.append(_observe(instants=(_at(10, 45), _at(10, 45, 1))))
    first_bytes = log.path.read_bytes()
    log.append(_observe(instants=(_at(13, 0), _at(13, 0, 1))))

    records = log.read()
    assert len(records) == 2
    assert log.path.read_bytes().startswith(first_bytes)  # earlier bytes untouched
    assert records[0]["candle"] == records[1]["candle"]
    assert (records[0]["requested_at"], records[1]["requested_at"]) == (
        "2026-10-05T10:45:00Z",
        "2026-10-05T13:00:00Z",
    )


def test_revisions_are_appended_as_new_observations(tmp_path: Path) -> None:
    log = UpstoxCandleEvidenceLog(tmp_path / "evidence.jsonl")
    for hour, close, volume, interest in (
        (10, 25180, 4200, 13_500_000),
        (12, 25190.5, 4200, 13_500_000),  # close revised
        (14, 25190.5, 4300, 13_500_000),  # volume revised
        (16, 25190.5, 4300, 13_600_000),  # open interest revised
        (18, 25200, 4400, 13_700_000),  # several fields
    ):
        log.append(
            _observe(
                [_candle(_MON, close=close, volume=volume * _LOT, open_interest=interest)],
                instants=(_at(hour), _at(hour, 0, 1)),
            )
        )

    candles = [record["candle"] for record in log.read()]
    assert [c["close"] for c in candles] == ["25180", "25190.5", "25190.5", "25190.5", "25200"]
    assert [c["open_interest"] for c in candles][-2:] == ["13600000", "13700000"]
    assert [c["volume"] for c in candles][1:3] == [str(4200 * _LOT), str(4300 * _LOT)]


def test_contracts_and_sessions_coexist_in_one_file(tmp_path: Path) -> None:
    log = UpstoxCandleEvidenceLog(tmp_path / "evidence.jsonl")
    log.append(_observe(day=_MON, contract=_NIFTY_OCT))
    log.append(_observe([_candle(_THU)], day=_THU, contract=_NIFTY_OCT))
    log.append(_observe(day=_MON, contract=_NIFTY_NOV))

    keys = [
        (r["contract"]["expiration"], r["trading_date"], r["instrument_key"]) for r in log.read()
    ]
    assert keys == [
        ("2026-10-27", "2026-10-05", _OCT_KEY),
        ("2026-10-27", "2026-10-01", _OCT_KEY),
        ("2026-11-24", "2026-10-05", _NOV_KEY),
    ]


def test_each_line_is_one_sorted_utf8_json_object(tmp_path: Path) -> None:
    log = UpstoxCandleEvidenceLog(tmp_path / "evidence.jsonl")
    log.append(_observe())

    data = log.path.read_bytes()
    assert data.endswith(b"\n") and data.count(b"\n") == 1
    record = json.loads(data.decode("utf-8"))
    assert list(record) == sorted(record)
    assert record["schema"] == _SCHEMA


def test_a_malformed_middle_line_is_refused(tmp_path: Path) -> None:
    log = UpstoxCandleEvidenceLog(tmp_path / "evidence.jsonl")
    log.append(_observe())
    with log.path.open("ab") as handle:
        handle.write(b"{not json\n")
    log.append(_observe())

    with pytest.raises(MalformedUpstoxCandleEvidenceError) as raised:
        log.read()
    assert raised.value.line_number == 2


def test_a_line_of_another_schema_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "evidence.jsonl"
    path.write_bytes(b'{"schema": "something-else/1"}\n')

    with pytest.raises(MalformedUpstoxCandleEvidenceError, match="line 1"):
        UpstoxCandleEvidenceLog(path).read()


def test_a_truncated_final_line_is_detected_and_never_appended_to(tmp_path: Path) -> None:
    log = UpstoxCandleEvidenceLog(tmp_path / "evidence.jsonl")
    log.append(_observe())
    with log.path.open("ab") as handle:
        handle.write(b'{"schema": "northstar.upstox-daily-candle-')
    before = log.path.read_bytes()

    with pytest.raises(TruncatedUpstoxCandleEvidenceError) as raised:
        log.read()
    assert raised.value.line_number == 2
    with pytest.raises(TruncatedUpstoxCandleEvidenceError):
        log.append(_observe())
    assert log.path.read_bytes() == before


def test_an_unusable_path_is_refused_before_writing(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        UpstoxCandleEvidenceLog(tmp_path / "missing" / "evidence.jsonl").require_appendable()
    with pytest.raises(IsADirectoryError):
        UpstoxCandleEvidenceLog(tmp_path).require_appendable()


def test_an_empty_or_missing_file_reads_as_no_records(tmp_path: Path) -> None:
    assert UpstoxCandleEvidenceLog(tmp_path / "absent.jsonl").read() == ()


# ---------------------------------------------------------------------------
# Token and boundaries
# ---------------------------------------------------------------------------


def test_the_token_never_leaves_the_authorization_header(tmp_path: Path) -> None:
    fetch = FakeFetch(candles=_success([_candle(_MON)]))
    collector = _collector(fetch, Clock(_at(10, 45), _at(10, 46)))
    log = UpstoxCandleEvidenceLog(tmp_path / "evidence.jsonl")

    log.append(collector.observe(_NIFTY_OCT, _MON))

    assert _TOKEN not in repr(collector)
    assert _TOKEN not in log.path.read_text(encoding="utf-8")
    (candle_call,) = fetch.candle_calls()
    url, headers, _ = candle_call
    assert headers["Authorization"] == f"Bearer {_TOKEN}"
    assert _TOKEN not in url
    assert "authorization" not in log.path.read_text(encoding="utf-8").lower()


def test_nothing_says_final_or_stable(tmp_path: Path) -> None:
    log = UpstoxCandleEvidenceLog(tmp_path / "evidence.jsonl")
    log.append(_observe())

    text = log.path.read_text(encoding="utf-8").lower()
    for word in ("final", "stable", "settled"):
        assert word not in text


def test_the_module_has_no_canonical_persistence_dependency() -> None:
    source = Path(evidence_module.__file__).read_text(encoding="utf-8")
    imported = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
        elif isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
    assert "sqlite3" not in imported
    assert not any("sqlite" in name or "persistence" in name for name in imported)
    assert not any(name.startswith("northstar_application") for name in imported)
    assert not any(name.startswith("northstar_api") for name in imported)
    # Identifiers and literals in code, not the explanatory docstring.
    tree = ast.parse(source)
    docstring = ast.get_docstring(tree)
    used = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}
    used |= {node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)}
    literals = {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and node.value != docstring
    }
    for forbidden in ("initialize_database", "FuturesOHLCVBar", "DatabaseOperationsLock"):
        assert forbidden not in used
    assert not any("Repository" in name or "Store" in name for name in used)
    assert not any("NORTHSTAR_" in text for text in literals)


def test_a_master_with_the_contract_resolves_its_current_lot_size() -> None:
    records = _master_records()
    for record in records:
        if record.get("instrument_key") == _OCT_KEY:
            record["lot_size"] = 75
    fetch = FakeFetch(master=_gzip_json(records), candles=_success([_candle(_MON, volume=75 * 10)]))

    record = _record(_collector(fetch, Clock(_at(10), _at(10, 0, 1))).observe(_NIFTY_OCT, _MON))

    assert (record["lot_size"], record["volume_contracts"]) == ("75", "10")
