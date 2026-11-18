"""Field Tier: Edge Nodes and the real-world entities they wrap.

Paper, S2.3: "The Field Tier ... is populated by Edge Nodes (EN): any kind of
device connected to the digital world on one side and to the real world to the
other.  ENs may have embedded intelligence (e.g., a smart machine) or not
(e.g., an IoT sensor or actuator); the FAR-EDGE RA honours this difference:
Smart Objects are ENs with on board computing capabilities, Connected Devices
are those without.  The Smart Object is where local control logic runs: it's a
semi-autonomous entity that does not need to interact too frequently with the
upper layers of the system."

And: "The Field is also populated by entities of the real world -- i.e.,
physical elements of production processes not directly connected to the network
... Things, People and Environments.  These are represented by some form of EN
wrapper.  For example, temperature (Environment) is measured by an IoT sensor
(Connected Device), a worker's proximity (People) to a physical checkpoint
location is published by an RFID wearable and detected by an RFID Gate
(Connected Device), and a conveyor belt (Thing) is operated by a PLC (Smart
Object)."

All three of those examples are instantiated by the platform builder, so the
RA's Field Tier taxonomy is represented literally rather than by analogy.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from ..common.crypto import Identity
from ..common.ledger_client import FieldLedgerClients
from ..common.messages import Command, DataMessage, Event, StreamRecord
from ..common.pdp import PolicyDecisionPoint
from .protocols import FieldProtocolAdapter

# Real-world entity classes of the RA.
THING = "THING"
PEOPLE = "PEOPLE"
ENVIRONMENT = "ENVIRONMENT"


@dataclass
class RealWorldEntity:
    """A physical element that is not itself connected to the network."""

    entity_id: str
    entity_class: str           # THING | PEOPLE | ENVIRONMENT
    description: str = ""
    wrapped_by: str = ""        # id of the Edge Node that represents it


class EdgeNode:
    """Base class of everything in the Field Tier."""

    has_onboard_compute = False

    def __init__(self, node_id: str, station: str,
                 adapter: FieldProtocolAdapter, node_type: str = "EN"):
        self.node_id = node_id
        self.station = station
        self.adapter = adapter
        self.node_type = node_type
        self.represents: List[RealWorldEntity] = []

    def wrap(self, entity: RealWorldEntity) -> RealWorldEntity:
        entity.wrapped_by = self.node_id
        self.represents.append(entity)
        return entity

    def descriptor(self) -> Dict[str, Any]:
        """Self-description used for plug-and-play registration in the global
        asset registry (Configuration Services)."""
        return {"type": self.node_type, "station": self.station,
                "protocol": self.adapter.name,
                "endpoint": self.adapter.address_of(self.node_id, "*"),
                "smart_object": self.has_onboard_compute,
                "represents": [e.entity_class for e in self.represents]}


# --------------------------------------------------------------------------
# Connected Devices -- no on-board computing capability
# --------------------------------------------------------------------------
class Sensor(EdgeNode):
    """An IoT sensor: publishes readings, holds no logic."""

    def __init__(self, node_id: str, station: str, adapter: FieldProtocolAdapter,
                 signals: List[str], units: Optional[Dict[str, str]] = None):
        super().__init__(node_id, station, adapter, node_type="SENSOR")
        self.signals = signals
        self.units = units or {}
        self.seq = 0
        self.readings_published = 0

    def publish_reading(self, signal: str, value: float,
                        t: Optional[float] = None) -> float:
        msg = DataMessage(source=self.node_id, signal=signal, value=float(value),
                          unit=self.units.get(signal, ""),
                          t=t if t is not None else time.time(), seq=self.seq)
        self.seq += 1
        self.readings_published += 1
        return self.adapter.publish(self.adapter.address_of(self.node_id, signal),
                                    msg)

    def publish_bundle(self, values: Dict[str, float],
                       t: Optional[float] = None,
                       channel: str = "raw") -> float:
        """Publish one high-rate, multi-signal record.

        This is a *stream* record, so it is the Data Routing & Pre-processing
        Component -- not Field Abstraction -- that is allowed to handle it.
        """
        rec = StreamRecord(source=self.node_id, channel=channel,
                           values={k: float(v) for k, v in values.items()},
                           t=t if t is not None else time.time(), seq=self.seq)
        self.seq += 1
        self.readings_published += 1
        return self.adapter.publish(
            self.adapter.address_of(self.node_id, channel), rec)

    def publish_event(self, name: str, severity: str = "INFO",
                      **payload: Any) -> float:
        ev = Event(source=self.node_id, name=name, payload=payload,
                   severity=severity)
        return self.adapter.publish(self.adapter.address_of(self.node_id, "events"),
                                    ev)


class Actuator(EdgeNode):
    """An actuator: accepts commands, holds no logic."""

    def __init__(self, node_id: str, station: str, adapter: FieldProtocolAdapter,
                 actions: List[str]):
        super().__init__(node_id, station, adapter, node_type="ACTUATOR")
        self.actions = actions
        self.state: Dict[str, Any] = {}
        self.commands_applied = 0
        self.commands_refused = 0

    def apply(self, command: Command) -> Dict[str, Any]:
        if command.action not in self.actions:
            self.commands_refused += 1
            return {"ok": False, "reason": f"unsupported action {command.action}"}
        self.state[command.action] = command.args
        self.state["last_action"] = command.action
        self.commands_applied += 1
        return {"ok": True, "action": command.action, "args": command.args}


class RfidGate(Sensor):
    """Detects an RFID wearable: the EN wrapper for a People entity."""

    def __init__(self, node_id: str, station: str, adapter: FieldProtocolAdapter):
        super().__init__(node_id, station, adapter, signals=["worker_present"])
        self.node_type = "RFID_GATE"

    def report_proximity(self, worker_id: str, present: bool,
                         t: Optional[float] = None) -> float:
        return self.publish_event("WorkerProximity",
                                  severity="INFO", worker=worker_id,
                                  present=bool(present), station=self.station)


# --------------------------------------------------------------------------
# Smart Objects -- on-board computing capability
# --------------------------------------------------------------------------
class SmartObject(EdgeNode):
    """A PLC-class device: on-board control logic, a PDP and Ledger Clients.

    Per S3.1 the Platform defines exactly two Components that run on such a
    device: a Policy Decision Point and Ledger Clients.  Both are wired in
    here.  "Typically, Smart Objects are controller boards with some
    significant processing power and a good network connection, but lack any
    (usable) local storage" -- so this class keeps only a small ring buffer and
    never persists anything.
    """

    has_onboard_compute = True
    LOCAL_BUFFER = 32

    def __init__(self, node_id: str, station: str, adapter: FieldProtocolAdapter,
                 pdp: Optional[PolicyDecisionPoint] = None,
                 ledger_clients: Optional[FieldLedgerClients] = None,
                 identity: Optional[Identity] = None):
        super().__init__(node_id, station, adapter, node_type="SMART_OBJECT")
        self.pdp = pdp                       # Security XC (Field Tier)
        self.ledger = ledger_clients         # Ledger Clients (Field Tier)
        self.identity = identity
        self.actuators: Dict[str, Actuator] = {}
        self.sensors: Dict[str, Sensor] = {}
        self._buffer: List[Dict[str, float]] = []

        # Local interlock limits: enforced on board, with no network round trip.
        self.interlocks: Dict[str, float] = {}
        self.local_trips = 0
        self.commands_denied = 0
        self.commands_executed = 0
        self.setpoints: Dict[str, Any] = {}
        self.stopped = False
        self.local_decision_latencies_ms: List[float] = []

    # -- composition ------------------------------------------------------
    def attach(self, node: EdgeNode) -> EdgeNode:
        if isinstance(node, Actuator):
            self.actuators[node.node_id] = node
        elif isinstance(node, Sensor):
            self.sensors[node.node_id] = node
        return node

    # -- Security XC: assess incoming calls -------------------------------
    def handle_command(self, command: Command) -> Dict[str, Any]:
        """Assess an incoming call against central policy, then execute it.

        "It supports Smart Objects in assessing incoming calls against security
        policies that are defined and maintained centrally, approving their
        execution only if they are legitimate." (S3.1)
        """
        resource = f"{self.station}/{command.target}"
        if self.pdp is not None:
            decision = self.pdp.evaluate(command.issuer or "anonymous",
                                         command.action, resource)
            if not decision.permitted:
                self.commands_denied += 1
                return {"ok": False, "reason": f"denied: {decision.reason}",
                        "policy": decision.policy_id}

        if command.action == "stop":
            self.stopped = True
        elif command.action == "start":
            self.stopped = False
        elif command.action in ("set_speed", "set_parameter", "reconfigure"):
            self.setpoints.update(command.args)

        target = self.actuators.get(command.target)
        result = target.apply(command) if target else {"ok": True,
                                                       "applied_on": self.node_id}
        self.commands_executed += 1
        return result

    # -- on-board control logic -------------------------------------------
    def set_interlock(self, signal: str, hard_limit: float) -> None:
        self.interlocks[signal] = hard_limit

    def on_sample(self, sample: Dict[str, float]) -> Optional[Event]:
        """Run the local control loop on one field sample.

        A Smart Object is "a semi-autonomous entity that does not need to
        interact too frequently with the upper layers of the system": a hard
        interlock violation is handled here, on board, with zero network
        latency, and only *reported* upwards.
        """
        started = time.perf_counter()
        self._buffer.append(sample)
        if len(self._buffer) > self.LOCAL_BUFFER:
            self._buffer.pop(0)

        tripped: Optional[Event] = None
        for signal, limit in self.interlocks.items():
            value = sample.get(signal)
            if value is not None and value > limit:
                self.stopped = True
                self.local_trips += 1
                for actuator in self.actuators.values():
                    actuator.apply(Command(target=actuator.node_id, action="stop",
                                           args={"cause": signal},
                                           issuer=self.node_id))
                tripped = Event(source=self.node_id, name="LocalInterlockTrip",
                                severity="ALARM",
                                payload={"signal": signal, "value": value,
                                         "limit": limit, "station": self.station})
                break
        self.local_decision_latencies_ms.append(
            (time.perf_counter() - started) * 1000.0)
        return tripped

    def stats(self) -> Dict[str, Any]:
        lat = self.local_decision_latencies_ms
        return {"node_id": self.node_id, "station": self.station,
                "local_trips": self.local_trips,
                "commands_executed": self.commands_executed,
                "commands_denied": self.commands_denied,
                "mean_local_decision_ms": round(sum(lat) / len(lat), 4) if lat else 0.0,
                "stopped": self.stopped}
