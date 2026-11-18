"""Field Abstraction Component (Edge Tier).

Paper, S3.2: "The Field Abstraction Component implements part of the Field
Abstraction & Data Routing XC Functions.  Its role is to decouple the Field
from the upper layers of the Platform, hiding the complexity of different
communication protocols and data models.  It only deals with events, commands
and data messages, exchanged in both directions with a minimum of latency; it
does not deal with data streams or any kind of massive data flow, as such
concerns belong to Data Routing."

Two design consequences are enforced in code:

  * `ingest` accepts Event and DataMessage but *refuses* StreamRecord, raising
    the caller's attention to the fact that streams belong to Data Routing.
  * the component keeps a protocol-agnostic endpoint registry, so upper layers
    address devices by logical id and never see a topic or a node id.
"""
from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from ..common.messages import Command, DataMessage, Event, StreamRecord
from ..common.pdp import PolicyDecisionPoint
from ..field.devices import EdgeNode, SmartObject
from ..field.protocols import FieldProtocolAdapter


class StreamNotHandledError(TypeError):
    """Raised when a data stream is pushed into Field Abstraction."""


@dataclass
class Endpoint:
    """One entry of the local registry of Field device endpoints."""

    node_id: str
    station: str
    node_type: str
    protocol: str
    address: str
    signals: List[str] = field(default_factory=list)
    actions: List[str] = field(default_factory=list)
    node: Any = None


class FieldAbstraction:
    """Protocol- and model-agnostic access to the Field, for one Gateway."""

    def __init__(self, gateway_id: str, adapter: FieldProtocolAdapter,
                 pdp: Optional[PolicyDecisionPoint] = None):
        self.gateway_id = gateway_id
        self.adapter = adapter
        self.pdp = pdp
        self.endpoints: Dict[str, Endpoint] = {}
        self._event_handlers: List[Callable[[Event], None]] = []
        self._data_handlers: List[Callable[[DataMessage], None]] = []
        self.events_in = 0
        self.data_in = 0
        self.commands_out = 0
        self.commands_denied = 0
        self.latencies_ms: List[float] = []

    # -- endpoint registry -----------------------------------------------
    def bind(self, node: EdgeNode) -> Endpoint:
        """Register a Field device and make it addressable by logical id."""
        endpoint = Endpoint(
            node_id=node.node_id, station=node.station,
            node_type=node.node_type, protocol=self.adapter.name,
            address=self.adapter.address_of(node.node_id, "*"),
            signals=list(getattr(node, "signals", []) or []),
            actions=list(getattr(node, "actions", []) or []),
            node=node)
        self.endpoints[node.node_id] = endpoint
        return endpoint

    def discover(self, station: Optional[str] = None) -> List[Endpoint]:
        return [e for e in self.endpoints.values()
                if station is None or e.station == station]

    # -- northbound: events and data messages ----------------------------
    def on_event(self, handler: Callable[[Event], None]) -> None:
        self._event_handlers.append(handler)

    def on_data(self, handler: Callable[[DataMessage], None]) -> None:
        self._data_handlers.append(handler)

    def ingest(self, message: Any) -> None:
        """Accept one northbound message from the Field."""
        if isinstance(message, StreamRecord):
            raise StreamNotHandledError(
                "data streams are handled by Data Routing & Pre-processing, "
                "not by Field Abstraction (paper S3.2)")
        if isinstance(message, Event):
            self.events_in += 1
            for handler in self._event_handlers:
                handler(message)
        elif isinstance(message, DataMessage):
            self.data_in += 1
            for handler in self._data_handlers:
                handler(message)
        else:
            raise TypeError(f"Field Abstraction cannot carry {type(message).__name__}")

    # -- southbound: commands --------------------------------------------
    def send_command(self, target: str, action: str,
                     args: Optional[Dict[str, Any]] = None,
                     issuer: str = "") -> Dict[str, Any]:
        """Issue a command to a Field device, with minimum latency.

        The Edge-Tier PDP guards the service point, as "its users are those
        service points exposed by Components and Enablers running on EGs"
        (S3.2).
        """
        started = time.perf_counter()
        endpoint = self.endpoints.get(target)
        if endpoint is None:
            return {"ok": False, "reason": f"unknown Field endpoint {target}"}

        resource = f"{endpoint.station}/{target}"
        if self.pdp is not None:
            decision = self.pdp.evaluate(issuer or self.gateway_id, action, resource)
            if not decision.permitted:
                self.commands_denied += 1
                return {"ok": False, "reason": f"denied: {decision.reason}"}

        command = Command(target=target, action=action, args=args or {},
                          issuer=issuer or self.gateway_id)
        owner = endpoint.node
        if isinstance(owner, SmartObject):
            sink = owner.handle_command
        elif hasattr(owner, "apply"):
            sink = owner.apply
        else:
            sink = lambda c: {"ok": True, "noop": True}

        _, result = self.adapter.write(endpoint.address, command, sink)
        self.commands_out += 1
        self.latencies_ms.append((time.perf_counter() - started) * 1000.0)
        return result if isinstance(result, dict) else {"ok": True}

    def stats(self) -> Dict[str, Any]:
        lat = self.latencies_ms
        return {"endpoints": len(self.endpoints), "events_in": self.events_in,
                "data_in": self.data_in, "commands_out": self.commands_out,
                "commands_denied": self.commands_denied,
                "mean_command_latency_ms": round(sum(lat) / len(lat), 3) if lat else 0.0}
