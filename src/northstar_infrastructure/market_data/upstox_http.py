"""HTTP access and error translation shared by the Upstox market-data adapters.

Only two Upstox resources are ever read, both with ``GET``: the public JSON
instrument master and the v3 historical-candle endpoint. No order, portfolio,
position, funds or any other trading endpoint is reachable from here, and no
request body is ever sent.

Credentials
-----------
The bearer token travels only in a request header handed to the fetch
callable. It is never part of a URL, never placed in an error message, and
never echoed from a provider response, so nothing raised here can carry it.

Numbers
-------
JSON is decoded with ``parse_float=Decimal``, so a provider price never passes
through a binary float. ``NaN`` and ``Infinity`` literals are refused at decode
time rather than becoming non-finite values downstream.

The edge in front of Upstox
---------------------------
Upstox is served through Cloudflare, which refuses urllib's default
``Python-urllib/<version>`` User-Agent with HTTP 403 (Cloudflare error 1010,
"browser signature banned") before the request ever reaches Upstox. Every
request therefore names this client explicitly. A 403 that Cloudflare itself
generated is reported as an access block rather than an authentication failure:
the credentials were never examined, and calling it a token problem sends the
diagnosis in the wrong direction.
"""

from __future__ import annotations

import gzip
import json
import re
import zlib
from collections.abc import Callable, Mapping
from decimal import Decimal
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

UpstoxFetch = Callable[[str, Mapping[str, str], float], bytes]
"""``fetch(url, headers, timeout) -> body`` performing one HTTP GET."""

_GZIP_MAGIC = b"\x1f\x8b"
_INVALID_INSTRUMENT_KEY = "UDAPI100011"
_USER_AGENT = "northstar-infrastructure/0.1"

# Cloudflare's plain-text error body, sent when JSON was not requested.
_EDGE_TEXT_ERROR = re.compile(r"^error code: (\d{4})$")


class UpstoxMarketDataSourceError(RuntimeError):
    """Raised when Upstox is unavailable or returns data Northstar cannot accept."""


class UpstoxInstrumentResolutionError(UpstoxMarketDataSourceError):
    """Raised when a FuturesContract does not map to exactly one usable Upstox instrument."""


class UpstoxInvalidInstrumentKeyError(UpstoxMarketDataSourceError):
    """Raised when Upstox rejects a resolved instrument key (``UDAPI100011``).

    This is what an expired contract's former key returns. It means the key
    resolved from the instrument master is no longer accepted by the API.
    """


class UpstoxAuthenticationError(UpstoxMarketDataSourceError):
    """Raised when Upstox refuses the request's credentials (HTTP 401 or 403)."""

    def __init__(self, message: str, status: int) -> None:
        super().__init__(message)
        self.status = status


class UpstoxAccessBlockedError(UpstoxMarketDataSourceError):
    """Raised when the Cloudflare edge in front of Upstox refuses a request.

    Upstox never saw the request, so this says nothing about the credentials.
    ``edge_error_code`` is Cloudflare's own ``1xxx`` code when it was reported;
    1010 means the client's signature, typically its User-Agent, is banned.
    Retrying unchanged will not help.
    """

    def __init__(self, message: str, status: int, edge_error_code: int | None) -> None:
        super().__init__(message)
        self.status = status
        self.edge_error_code = edge_error_code


class UpstoxProviderUnavailableError(UpstoxMarketDataSourceError):
    """Raised when Upstox cannot currently serve a request.

    Covers rate limiting (HTTP 429), provider failures (HTTP 5xx), transport
    failures and undecodable responses. ``status`` is the HTTP status when one
    was received, so a caller can tell rate limiting apart without parsing a
    message.
    """

    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


def default_fetch(url: str, headers: Mapping[str, str], timeout: float) -> bytes:
    """Perform one HTTP GET and return the raw response body.

    An explicit User-Agent is always sent unless the caller supplies one; see
    the module docstring for why urllib's default cannot be used.
    """
    request = Request(url, headers={"User-Agent": _USER_AGENT, **headers}, method="GET")
    with urlopen(request, timeout=timeout) as response:
        return response.read()


def get_json(
    fetch: UpstoxFetch,
    url: str,
    headers: Mapping[str, str],
    timeout: float,
    *,
    context: str,
) -> Any:
    """Fetch and decode one JSON resource, translating every failure.

    ``context`` names what was being fetched in Northstar terms. It must not
    contain a credential; the URL is deliberately not included in any message.
    """
    try:
        raw = fetch(url, headers, timeout)
    except HTTPError as exc:
        raise _translate_http_error(exc, context) from exc
    except (URLError, TimeoutError, OSError) as exc:
        raise UpstoxProviderUnavailableError(
            f"Upstox provider is unavailable for {context}."
        ) from exc
    return decode_json(raw, context=context)


def decode_json(raw: object, *, context: str) -> Any:
    """Decode a JSON body, transparently gunzipping the instrument-master files."""
    if not isinstance(raw, bytes | bytearray):
        raise UpstoxProviderUnavailableError(
            f"Upstox returned a non-binary response body for {context}."
        )
    try:
        body = bytes(raw)
        if body[:2] == _GZIP_MAGIC:
            body = gzip.decompress(body)
        return json.loads(body, parse_float=Decimal, parse_constant=_refuse_constant)
    except (OSError, EOFError, zlib.error, UnicodeDecodeError, ValueError) as exc:
        raise UpstoxProviderUnavailableError(
            f"Upstox returned an undecodable response for {context}."
        ) from exc


def provider_error_codes(payload: object) -> tuple[str, ...]:
    """Return the Upstox error codes in an error payload, in provider order."""
    if not isinstance(payload, dict):
        return ()
    errors = payload.get("errors")
    if not isinstance(errors, list):
        return ()
    codes: list[str] = []
    for error in errors:
        if not isinstance(error, dict):
            continue
        code = error.get("errorCode", error.get("error_code"))
        if isinstance(code, str) and code:
            codes.append(code)
    return tuple(codes)


def _translate_http_error(exc: HTTPError, context: str) -> UpstoxMarketDataSourceError:
    status = exc.code
    payload, text = _error_body(exc)
    codes = provider_error_codes(payload)
    described = f" (provider codes {', '.join(codes)})" if codes else ""

    if status == 403 and not codes:
        blocked, edge_code = _edge_block(payload, text)
        if blocked:
            named = f" (Cloudflare error {edge_code})" if edge_code is not None else ""
            return UpstoxAccessBlockedError(
                f"The Cloudflare edge in front of Upstox blocked the request for {context}"
                f"{named}; Upstox did not evaluate the credentials (HTTP 403).",
                status,
                edge_code,
            )
    if status == 400 and _INVALID_INSTRUMENT_KEY in codes:
        return UpstoxInvalidInstrumentKeyError(
            f"Upstox rejected the resolved instrument key for {context} as invalid "
            f"({_INVALID_INSTRUMENT_KEY}); the contract is no longer served under it."
        )
    if status in (401, 403):
        return UpstoxAuthenticationError(
            f"Upstox refused the credentials for {context} (HTTP {status}){described}.",
            status,
        )
    if status == 429:
        return UpstoxProviderUnavailableError(
            f"Upstox provider is unavailable for {context}: rate limited (HTTP 429).",
            status,
        )
    if status >= 500:
        return UpstoxProviderUnavailableError(
            f"Upstox provider is unavailable for {context} (HTTP {status}).",
            status,
        )
    return UpstoxMarketDataSourceError(
        f"Upstox rejected the request for {context} (HTTP {status}){described}."
    )


def _error_body(exc: HTTPError) -> tuple[object, str]:
    """Best-effort read of an error body as (decoded JSON or None, stripped text)."""
    try:
        body = exc.read()
    except Exception:
        return None, ""
    if not body:
        return None, ""
    try:
        text = body.decode("utf-8").strip()
    except UnicodeDecodeError:
        return None, ""
    try:
        return json.loads(text, parse_float=Decimal, parse_constant=_refuse_constant), text
    except ValueError:
        return None, text


def _edge_block(payload: object, text: str) -> tuple[bool, int | None]:
    """Recognise an error Cloudflare generated itself, in either of its formats.

    JSON (sent when JSON was accepted) marks itself with ``cloudflare_error``
    and an integer ``error_code``; Upstox's own ``error_code`` values are
    ``UDAPI...`` strings, so the two cannot be confused. Plain text reads
    exactly ``error code: 1010``.
    """
    if isinstance(payload, dict) and payload.get("cloudflare_error") is True:
        code = payload.get("error_code")
        valid = isinstance(code, int) and not isinstance(code, bool)
        return True, code if valid else None
    match = _EDGE_TEXT_ERROR.match(text)
    if match:
        return True, int(match.group(1))
    return False, None


def _refuse_constant(name: str) -> Any:
    raise ValueError(f"Non-finite JSON constant {name} is not accepted.")
