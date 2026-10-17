"""The production trust anchor: NSE reference facts the product owner has accepted.

This module is the only place real accepted evidence may live. It is
deliberately small and reviewed by hand, like the NSE calendar reference.

It is EMPTY. No extracted fact (NSE-F-01 to NSE-F-22 in the docs evidence
register) has been accepted, so no historical NSE reference fact is trusted
and no exchange-published authority can be produced.

Adding an accepted fact
-----------------------
Only after the owner has explicitly accepted that exact fact, following the
review gate in ``northstar-docs/research/NSE-Historical-Futures-Reference-Evidence-Register.md``:

1. The owner's decision is recorded in the register: decision ID, date,
   purpose, accepted interpretation and limitations.
2. One ``HistoricalFuturesAcceptedFact`` is added below for that fact only,
   with the same fact ID, evidence ID, document identity, category, contract,
   value, boundaries and decision. Nothing is accepted for a whole document.
3. ``version`` is bumped, so every cutoff authority names the registry that
   granted it.
4. A regression test pins the added fact against the register.

Never add a fact with a guessed value, an inferred scope, an invented
decision or an unresolved condition left out. Never add test fixtures here.

This is a code-review boundary, not a cryptographic one: anyone who can edit
this file can change it. Review of this file is the control.
"""

from __future__ import annotations

from northstar_infrastructure.market_data.historical_futures_reference_evidence import (
    HistoricalFuturesAcceptedEvidenceRegistry,
)

PRODUCTION_ACCEPTED_EVIDENCE = HistoricalFuturesAcceptedEvidenceRegistry(
    version="nse-accepted-evidence/0",
    facts=(),
)
