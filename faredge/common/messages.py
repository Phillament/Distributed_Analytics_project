"""Message types exchanged across the FAR-EDGE Platform.

The paper is precise about what the Field Abstraction Component carries:
"It only deals with events, commands and data messages, exchanged in both
directions with a minimum of latency; it does not deal with data streams or
any kind of massive data flow, as such concerns belong to Data Routing"
(S3.2).  The three classes below are therefore first-class citizens of this
implementation, and StreamRecord is deliberately a *separate* type routed
only by the Data Routing & Pre-processing Component.
"""
from __future__ import annotations

import itertools
import time
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, Optional

from .crypto import canonical_json

_counter = itertools.count(1)


def _next_id(prefix: str) -> str:
    return f"{prefix}-{next(_counter):08d}"


class _Wire:
    """Mixin giving every message an on-the-wire size.

    Byte accounting is what lets us quantify the paper's claim that edge
    nodes save bandwidth by filtering field data (S1.2).
    """

    def wire_size(self) -> int:
        return len(canonical_json(asdict(self)).encode("utf-8"))


@dataclass
class DataMessage(_Wire):
    """A single, self-contained reading produced by a Field entity."""

    source: str                       # Edge Node id
    signal: str                       # e.g. vibration_mm_s
    value: float
    unit: str = ""
    t: float = field(default_factory=time.time)
    seq: int = 0
    id: str = field(default_factory=lambda: _next_id("dm"))
    kind: str = "DATA"


@dataclass
class Event(_Wire):
    """Something notable happened in the Field (threshold crossed, part done,
    device joined).  Events travel northbound with minimum latency."""

    source: str
    name: str
    payload: Dict[str, Any] = field(default_factory=dict)
    severity: str = "INFO"            # INFO | WARN | ALARM
    t: float = field(default_factory=time.time)
    id: str = field(default_factory=lambda: _next_id("ev"))
    kind: str = "EVENT"


@dataclass
class Command(_Wire):
    """A control action sent southbound to a Field entity."""

    target: str                       # Edge Node id
    action: str                       # e.g. set_speed, stop, reconfigure
    args: Dict[str, Any] = field(default_factory=dict)
    issuer: str = ""                  # subject, used by the PDP
    t: float = field(default_factory=time.time)
    id: str = field(default_factory=lambda: _next_id("cmd"))
    kind: str = "COMMAND"


@dataclass
class StreamRecord(_Wire):
    """A record belonging to a massive, unidirectional data flow.

    Handled exclusively by the Data Routing & Pre-processing Component, which
    is "less concerned with time constraints and more with data throughput"
    (paper S3.2).
    """

    source: str
    channel: str
    values: Dict[str, float] = field(default_factory=dict)
    t: float = field(default_factory=time.time)
    seq: int = 0
    kind: str = "STREAM"


# --------------------------------------------------------------------------
# Ledger Tier message types
# --------------------------------------------------------------------------
TX_APPEND = "APPEND"
TX_AMEND = "AMEND"
TX_REMOVE = "REMOVE"
TX_DEPLOY = "DEPLOY_CONTRACT"


@dataclass
class ServiceCall(_Wire):
    """A call on a Ledger Service endpoint.

    "These are transaction-oriented: each service call, which needs to modify
    the shared state of a system, must be evaluated and approved by Peer Nodes
    before taking effect." (paper S2.3)
    """

    service: str                      # e.g. DataPublishingServices
    endpoint: str                     # e.g. submitLocalKPI
    args: Dict[str, Any] = field(default_factory=dict)
    caller: str = ""                  # address of the calling Ledger Client
    op: str = TX_APPEND               # APPEND | AMEND | REMOVE | DEPLOY
    t: float = field(default_factory=time.time)
    id: str = field(default_factory=lambda: _next_id("call"))
    kind: str = "SERVICE_CALL"

    def digest(self) -> Dict[str, Any]:
        """The part of the call that is signed, hashed and executed.

        The caller's timestamp is included so that it is covered by the
        signature *and* so that every Peer Node executes the contract with the
        identical notion of "now" -- a precondition for determinism, since a
        contract may never read the wall clock itself.
        """
        return {
            "service": self.service,
            "endpoint": self.endpoint,
            "args": self.args,
            "caller": self.caller,
            "op": self.op,
            "id": self.id,
            "t": self.t,
        }


@dataclass
class LedgerEvent(_Wire):
    """Event emitted by a smart contract once a transaction commits.

    Edge-Tier Ledger Clients can subscribe to these; the Field-Tier variant
    cannot -- the Edge variant is described as providing the same
    functionality "but with the additional capability of subscribing to
    ledger events" (paper S3.2).
    """

    service: str
    name: str
    payload: Dict[str, Any] = field(default_factory=dict)
    block: int = -1
    t: float = field(default_factory=time.time)
    kind: str = "LEDGER_EVENT"


@dataclass
class Receipt:
    """Outcome handed back to the caller of a Ledger Service."""

    call_id: str
    accepted: bool
    block_index: int = -1
    votes_for: int = 0
    votes_against: int = 0
    peers_online: int = 0
    reason: str = ""
    result: Optional[Dict[str, Any]] = None
    latency_ms: float = 0.0
