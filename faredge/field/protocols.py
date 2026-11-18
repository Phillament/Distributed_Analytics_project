"""Field communication protocols.

Paper, S3.2: "The on-going FAR-EDGE implementation supports the OPC UA and the
MQTT field communication protocols."

Both are modelled here behind one adapter interface, because the point of the
Field Abstraction Component is precisely to "decouple the Field from the upper
layers of the Platform, hiding the complexity of different communication
protocols and data models".  The two adapters differ in:

  * addressing  -- MQTT uses a topic tree, OPC UA uses namespaced node ids;
  * overhead    -- an OPC UA binary session frame costs more bytes per message
                   than an MQTT PUBLISH with a short topic;
  * subscription-- MQTT has wildcard topic subscriptions, OPC UA has
                   monitored items.

Everything above the adapter sees only Events, Commands, DataMessages and
StreamRecords, so a station can be switched from MQTT to OPC UA without any
change to the Edge, Ledger or Cloud Tiers -- which the test suite checks.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Callable, Dict, List, Tuple

from ..common.messages import Command, DataMessage, Event, StreamRecord
from ..common.network import Link

Handler = Callable[[Any], None]


class FieldProtocolAdapter:
    """Common interface of a field protocol binding."""

    name = "ABSTRACT"
    header_bytes = 0

    def __init__(self, link: Link):
        self._link = link
        self._subscribers: Dict[str, List[Handler]] = defaultdict(list)
        self.published = 0
        self.written = 0

    # -- addressing ------------------------------------------------------
    def address_of(self, device_id: str, signal: str) -> str:
        raise NotImplementedError

    def _matches(self, pattern: str, address: str) -> bool:
        return pattern in ("*", address)

    # -- northbound ------------------------------------------------------
    def publish(self, address: str, message: Any) -> float:
        """Send a message from the Field towards the Edge Tier."""
        size = message.wire_size() + self.header_bytes
        latency = self._link.transmit(size, getattr(message, "kind", "DATA"))
        self.published += 1
        for pattern, handlers in list(self._subscribers.items()):
            if self._matches(pattern, address):
                for handler in handlers:
                    handler(message)
        return latency

    def subscribe(self, pattern: str, handler: Handler) -> None:
        self._subscribers[pattern].append(handler)

    # -- southbound ------------------------------------------------------
    def write(self, address: str, command: Command,
              sink: Callable[[Command], Any]) -> Tuple[float, Any]:
        """Send a command from the Edge Tier down to the Field."""
        size = command.wire_size() + self.header_bytes
        latency = self._link.transmit(size, "COMMAND")
        self.written += 1
        return latency, sink(command)

    def stats(self) -> Dict[str, Any]:
        return {"protocol": self.name, "published": self.published,
                "written": self.written, "header_bytes": self.header_bytes}


class MqttAdapter(FieldProtocolAdapter):
    """MQTT binding: hierarchical topics, small fixed header, wildcards."""

    name = "MQTT"
    header_bytes = 28          # fixed header + short topic + packet id

    def __init__(self, link: Link, root: str = "plant/line1"):
        super().__init__(link)
        self.root = root

    def address_of(self, device_id: str, signal: str) -> str:
        return f"{self.root}/{device_id}/{signal}"

    def _matches(self, pattern: str, address: str) -> bool:
        """Support the MQTT '+' (single level) and '#' (multi level) wildcards."""
        if pattern == "*":
            return True
        p, a = pattern.split("/"), address.split("/")
        for i, token in enumerate(p):
            if token == "#":
                return True
            if i >= len(a):
                return False
            if token not in ("+", a[i]):
                return False
        return len(p) == len(a)


class OpcUaAdapter(FieldProtocolAdapter):
    """OPC UA binding: namespaced node ids, binary session frames, monitored
    items."""

    name = "OPCUA"
    header_bytes = 76          # secure-channel + session + sequence headers

    def __init__(self, link: Link, namespace: int = 2):
        super().__init__(link)
        self.namespace = namespace

    def address_of(self, device_id: str, signal: str) -> str:
        return f"ns={self.namespace};s={device_id}.{signal}"

    def _matches(self, pattern: str, address: str) -> bool:
        """A monitored item may address a single node or a whole device."""
        if pattern in ("*", address):
            return True
        if pattern.endswith(".*"):
            return address.startswith(pattern[:-1])
        return False


def make_adapter(protocol: str, link: Link) -> FieldProtocolAdapter:
    protocol = (protocol or "MQTT").upper()
    if protocol == "OPCUA":
        return OpcUaAdapter(link)
    if protocol == "MQTT":
        return MqttAdapter(link)
    raise ValueError(f"unsupported field protocol {protocol}")
