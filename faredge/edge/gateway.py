"""Edge Gateway -- the host of the Edge Tier.

Paper, S2.3: "The Edge Tier is in Plant Scope, located above the Field Tier
and below the Cloud Tier and is the core of the FAR-EDGE RA. ... The Edge Tier
is populated by Edge Gateways (EG) i.e. computing devices that act as a
digital world gateway to the real world.  Strategically positioned close to
physical systems, the EG can execute Edge Processes: time- and
bandwidth-critical functionality having local scope."

Paper, S3.2: "At the Platform level, edge computing is supported by seven
Components, two of them with Enabler status: Edge Automation Services, Edge
Analytics Engine, Field Abstraction, Data Routing & Pre-processing, Policy
Decision Point, Ledger Clients and Node Management.  All Components are meant
to be deployed on Edge Gateway (EG) machines."

All seven are instantiated in `__init__` and listed in `COMPONENTS`, which the
conformance experiment checks against the paper.
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional

from ..common.crypto import Identity
from ..common.ledger_client import EdgeLedgerClients
from ..common.messages import DataMessage, Event, LedgerEvent, StreamRecord
from ..common.network import FIELD_LAN, PLANT_LAN, WAN_CLOUD, Link
from ..common.pdp import PolicyAdministrationPoint, PolicyDecisionPoint
from ..config import Domain, PlatformConfig, Scope, Tier, XC
from ..field.devices import (Actuator, RfidGate, Sensor, SmartObject,
                             RealWorldEntity, ENVIRONMENT, PEOPLE, THING)
from ..field.protocols import make_adapter
from .analytics_engine import (EdgeAnalyticsEngine, EwmaAnomalyDetector,
                               LocalResult, StationKpiAlgorithm)
from .automation_services import (EdgeAutomationServices, ReconfigurationWorkflow,
                                  SafetyReactionWorkflow, StationCycleWorkflow)
from .data_routing import (DEST_CLOUD, DEST_EDGE, DataRouting, DeadBandFilter,
                           Decimator, ThresholdDetector, WindowAggregator)
from .field_abstraction import FieldAbstraction
from .node_management import NodeManagement

# The seven Edge-Tier Components of the Platform Design (paper S3.2, Fig. 2).
COMPONENTS = (
    ("Edge Automation Services", "ENABLER", Domain.AUTOMATION),
    ("Edge Analytics Engine", "ENABLER", Domain.ANALYTICS),
    ("Field Abstraction", "COMPONENT", XC.FIELD_ABSTRACTION_DATA_ROUTING),
    ("Data Routing & Pre-processing", "COMPONENT", XC.FIELD_ABSTRACTION_DATA_ROUTING),
    ("Policy Decision Point", "COMPONENT", XC.SECURITY),
    ("Ledger Clients", "COMPONENT", "ALL_DOMAINS"),
    ("Node Management", "COMPONENT", XC.MANAGEMENT),
)

RAW_TO_ANALYTICS = "raw->analytics"
RAW_TO_CLOUD = "raw->cloud"


@dataclass
class StationProfile:
    """What the gateway needs to know about the station it drives."""

    station: str
    ideal_cycle_time_s: float
    nominal_power_kw: float
    vibration_warn: float
    vibration_trip: float


class EdgeGateway:
    """One Edge Gateway, with all seven Edge-Tier Components deployed on it."""

    tier = Tier.EDGE
    scope = Scope.PLANT

    def __init__(self, gateway_id: str, profile: StationProfile,
                 cfg: PlatformConfig, ledger, links: Dict[str, Link],
                 pap: PolicyAdministrationPoint,
                 cloud_sink: Optional[Callable[[StreamRecord], None]] = None,
                 logger: Optional[Callable[[str], None]] = None):
        self.gateway_id = gateway_id
        self.profile = profile
        self.station = profile.station
        self.cfg = cfg
        self._links = links
        self._logger = logger
        self.identity = Identity(gateway_id, bits=cfg.ledger.rsa_key_bits)

        # ---- Component 5: Policy Decision Point (Security XC) -----------
        # Policies are administered centrally, so a cache miss costs a WAN
        # round trip to the Cloud Tier's Security Management Component.
        self.pdp = PolicyDecisionPoint(gateway_id, "EDGE", pap, links[WAN_CLOUD])

        # ---- Component 6: Ledger Clients (all Functional Domains) -------
        self.ledger_clients = EdgeLedgerClients(gateway_id, ledger,
                                                self.identity, links[PLANT_LAN])

        # ---- Field protocol binding -------------------------------------
        self.adapter = make_adapter(cfg.field_tier.protocol, links[FIELD_LAN])

        # ---- Component 3: Field Abstraction -----------------------------
        self.field_abstraction = FieldAbstraction(gateway_id, self.adapter,
                                                  self.pdp)

        # ---- Component 4: Data Routing & Pre-processing -----------------
        self.data_routing = DataRouting(gateway_id)

        # ---- Component 2: Edge Analytics Engine (Enabler) ---------------
        self.analytics = EdgeAnalyticsEngine(gateway_id,
                                             cfg.edge.kpi_publish_epsilon)

        # ---- Component 1: Edge Automation Services (Enabler) ------------
        self.automation = EdgeAutomationServices(
            gateway_id, self.station, self.field_abstraction, self.pdp,
            self.ledger_clients, logger)

        # ---- Component 7: Node Management (Management XC) ---------------
        self.node_management = NodeManagement(gateway_id, self.metrics)

        self.cloud_sink = cloud_sink
        self.field_nodes: Dict[str, Any] = {}
        self.entities: List[RealWorldEntity] = []
        self.samples_fed = 0
        self.kpi_publications = 0
        self.model_observations = 0
        self._last_model_report = 0
        self._build_field_tier()
        self._wire()

    # ------------------------------------------------------------------
    # Field Tier construction
    # ------------------------------------------------------------------
    def _build_field_tier(self) -> None:
        """Instantiate the Edge Nodes of this station and the real-world
        entities they wrap (paper S2.3, S3.1)."""
        st = self.station

        # Smart Object: the PLC that operates the conveyor (a Thing).
        self.plc = SmartObject(
            f"{st}-PLC", st, self.adapter,
            pdp=PolicyDecisionPoint(f"{st}-PLC", "FIELD",
                                    self.pdp.pap, self._links[WAN_CLOUD]),
            ledger_clients=None, identity=Identity(f"{st}-PLC", bits=256))
        self.plc.set_interlock("vibration_mm_s", self.profile.vibration_trip)
        self.entities.append(self.plc.wrap(RealWorldEntity(
            f"{st}-conveyor", THING, "conveyor belt operated by a PLC")))

        # Connected Devices: process sensor, environment sensor, RFID gate,
        # drive actuator.
        self.process_sensor = Sensor(
            f"{st}-SENS", st, self.adapter,
            signals=["vibration_mm_s", "power_kw", "motor_current_a",
                     "running", "part_done", "part_ok", "cycle_time_s"],
            units={"vibration_mm_s": "mm/s", "power_kw": "kW",
                   "motor_current_a": "A", "cycle_time_s": "s"})
        self.env_sensor = Sensor(f"{st}-ENV", st, self.adapter,
                                 signals=["temperature_c"],
                                 units={"temperature_c": "degC"})
        self.entities.append(self.env_sensor.wrap(RealWorldEntity(
            f"{st}-ambient", ENVIRONMENT, "ambient temperature")))
        self.rfid_gate = RfidGate(f"{st}-RFID", st, self.adapter)
        self.entities.append(self.rfid_gate.wrap(RealWorldEntity(
            f"{st}-operator", PEOPLE, "operator proximity via RFID wearable")))
        self.drive = Actuator(f"{st}-DRV", st, self.adapter,
                              actions=["start", "stop", "set_speed",
                                       "reconfigure"])
        self.plc.attach(self.drive)
        self.plc.attach(self.process_sensor)

        for node in (self.plc, self.process_sensor, self.env_sensor,
                     self.rfid_gate, self.drive):
            self.field_nodes[node.node_id] = node
            self.field_abstraction.bind(node)

    # ------------------------------------------------------------------
    # Wiring of the seven Components
    # ------------------------------------------------------------------
    def _wire(self) -> None:
        # Field -> Edge: the adapter demultiplexes by message type, honouring
        # the division of labour between Field Abstraction (events, commands,
        # data messages) and Data Routing (streams).
        self.adapter.subscribe("*", self._on_field_message)

        # Channel 1: raw stream -> local Analytics.  Destination is the Edge
        # Tier itself, so only the cheap stages are used and nothing is thrown
        # away that the KPI algorithm needs.
        self.threshold = ThresholdDetector(
            {"vibration_mm_s": self.profile.vibration_warn},
            sink=self._on_threshold_event)
        self.data_routing.open_channel(
            RAW_TO_ANALYTICS, producer=self.process_sensor.node_id,
            consumer=f"{self.gateway_id}/analytics", destination=DEST_EDGE,
            stages=[self.threshold], sink=self.analytics.southbound)

        # Channel 2: raw stream -> Cloud Tier.  "filtering, in order to save
        # network resources, when the final destination of the information flow
        # is in the Cloud" (S3.2).
        self.data_routing.open_channel(
            RAW_TO_CLOUD, producer=self.process_sensor.node_id,
            consumer="cloud/data-storage", destination=DEST_CLOUD,
            stages=[DeadBandFilter(self.cfg.edge.deadband_fraction),
                    Decimator(max(1, self.cfg.edge.analytics_window // 10)),
                    WindowAggregator(self.cfg.edge.analytics_window)],
            sink=self._to_cloud)

        # Events from the Field reach the automation workflows.
        self.field_abstraction.on_event(self.automation.dispatch_field_event)

        # Analytics algorithms.
        self.analytics.deploy(StationKpiAlgorithm(
            self.station, self.cfg.field_tier.sample_rate_hz,
            self.profile.ideal_cycle_time_s))
        self.analytics.deploy(EwmaAnomalyDetector(
            "vibration_mm_s", on_anomaly=self._on_threshold_event))

        # Ledger events reach the automation workflows (Edge-Tier Ledger
        # Clients can subscribe; Field-Tier ones cannot).
        self.ledger_clients.on("ParameterChanged",
                               self.automation.dispatch_ledger_event)
        self.ledger_clients.on("RecipeApplied",
                               self.automation.dispatch_ledger_event)

    # ------------------------------------------------------------------
    # message plumbing
    # ------------------------------------------------------------------
    def _on_field_message(self, message: Any) -> None:
        if isinstance(message, StreamRecord):
            # Streams belong to Data Routing, not to Field Abstraction.
            for channel_id in (RAW_TO_ANALYTICS, RAW_TO_CLOUD):
                self.data_routing.route(channel_id, message)
        else:
            self.field_abstraction.ingest(message)

    def _on_threshold_event(self, event: Event) -> None:
        """A locally detected condition: handled locally, on the gateway."""
        self.automation.dispatch_field_event(event)

    def _to_cloud(self, record: StreamRecord) -> None:
        """Hand a pre-processed record to the Cloud Tier.

        This is the only path on which field-derived data leaves Plant Scope,
        so it is charged to the WAN link: what the Data Routing pipeline
        filtered out is bandwidth that is never spent.
        """
        if self.cloud_sink is None:
            return
        self._links[WAN_CLOUD].transmit(record.wire_size(), "PREPROCESSED_STREAM")
        self.cloud_sink(record)

    # ------------------------------------------------------------------
    # deployment of workflows and registration on the ledger
    # ------------------------------------------------------------------
    def commission(self, work_order: str, is_first_station: bool) -> None:
        """Register in the global asset registry and start the workflows.

        Plug-and-play: the gateway announces itself and its Field devices to
        Configuration Services, so the whole platform learns about them through
        one consensus-backed registry.
        """
        for node in self.field_nodes.values():
            descriptor = node.descriptor()
            self.ledger_clients.configuration.register_asset(
                node.node_id, gateway=self.gateway_id,
                signals=list(getattr(node, "signals", []) or []),
                capabilities=list(getattr(node, "actions", []) or []),
                **{k: v for k, v in descriptor.items()
                   if k in ("type", "station", "protocol", "endpoint")})

        # Digital model of the machine, for the Digital Models XC Functions.
        self.ledger_clients.synchronization.register_model(
            self.station,
            params={"nominal_power_kw": self.profile.nominal_power_kw,
                    "ideal_cycle_time_s": self.profile.ideal_cycle_time_s},
            thresholds={"nominal_power_kw": 0.05, "ideal_cycle_time_s": 0.05},
            entity=f"{self.station}-machine")

        self.automation.deploy(StationCycleWorkflow(self.station, work_order,
                                                    is_first_station))
        self.automation.deploy(SafetyReactionWorkflow(self.station,
                                                      self.drive.node_id))
        self.automation.deploy(ReconfigurationWorkflow(self.station,
                                                       self.drive.node_id))
        self.node_management.commission()

    def decommission(self) -> None:
        self.automation.stop_all()
        self.node_management.decommission()

    # ------------------------------------------------------------------
    # the hot loop: one Field sample
    # ------------------------------------------------------------------
    def feed(self, row: Dict[str, float]) -> None:
        """Process one sample of the station's field data.

        Order of operations mirrors a real cycle:
          1. the Smart Object runs its on-board interlock logic (no network),
          2. the process sensor publishes the raw bundle to the gateway,
          3. the environment sensor publishes its slow signal,
          4. a completed operation is announced as an Event.
        """
        self.samples_fed += 1
        values = {k: float(v) for k, v in row.items()
                  if k in ("vibration_mm_s", "power_kw", "motor_current_a",
                           "running", "part_done", "part_ok", "cycle_time_s")}
        t = float(row.get("t", time.time()))

        trip = self.plc.on_sample(values)
        self.process_sensor.publish_bundle(values, t=t)

        if self.samples_fed % int(max(1, self.cfg.field_tier.sample_rate_hz)) == 0:
            self.env_sensor.publish_reading("temperature_c",
                                            float(row.get("temperature_c", 0.0)), t=t)
        if trip is not None:
            self.adapter.publish(
                self.adapter.address_of(self.plc.node_id, "events"), trip)
        if values.get("part_done", 0.0) >= 0.5:
            self.adapter.publish(
                self.adapter.address_of(self.plc.node_id, "events"),
                Event(source=self.plc.node_id, name="OperationFinished",
                      payload={"cycle_time_s": values.get("cycle_time_s", 0.0),
                               "ok": values.get("part_ok", 1.0) >= 0.5,
                               "station": self.station}, t=t))

    # ------------------------------------------------------------------
    # northbound publication
    # ------------------------------------------------------------------
    def publish_kpis(self, force: bool = False) -> int:
        """Contribute local analytics results to the global KPIs."""
        def publisher(result: LocalResult) -> bool:
            receipt = self.ledger_clients.data_publishing.submit_local_kpi(
                self.gateway_id, result.kpi, value=result.value,
                weight=result.weight, numerator=result.numerator,
                denominator=result.denominator)
            return receipt.accepted

        published = self.analytics.northbound(publisher, force=force)
        self.kpi_publications += published
        return published

    def synchronise_model(self) -> int:
        """Report observed machine behaviour to Synchronization Services."""
        kpi = self.analytics.algorithms.get("station_kpi")
        if kpi is None or kpi.samples == 0:
            return 0
        reports = 0
        observed_power = (kpi.energy_kwh * 3600.0
                          / max(1e-9, kpi.planned_time_s))
        receipt = self.ledger_clients.synchronization.report_observation(
            self.station, "nominal_power_kw", observed_power, n=kpi.samples)
        reports += int(receipt.accepted)
        if kpi.parts_total:
            receipt = self.ledger_clients.synchronization.report_observation(
                self.station, "ideal_cycle_time_s", kpi.mean_cycle_time_s,
                n=kpi.parts_total)
            reports += int(receipt.accepted)
        self.model_observations += reports
        return reports

    # ------------------------------------------------------------------
    # introspection
    # ------------------------------------------------------------------
    def metrics(self) -> Dict[str, Any]:
        return {"samples_fed": self.samples_fed,
                "kpi_publications": self.kpi_publications,
                "local_kpis": {k: round(v, 6)
                               for k, v in self.analytics.local_kpis().items()},
                "data_routing": self.data_routing.totals(),
                "pdp": self.pdp.stats.as_dict()}

    def component_inventory(self) -> List[Dict[str, str]]:
        """Report the seven Components, for the conformance experiment."""
        instances = {
            "Edge Automation Services": self.automation,
            "Edge Analytics Engine": self.analytics,
            "Field Abstraction": self.field_abstraction,
            "Data Routing & Pre-processing": self.data_routing,
            "Policy Decision Point": self.pdp,
            "Ledger Clients": self.ledger_clients,
            "Node Management": self.node_management,
        }
        return [{"component": name, "status": status, "realises": realises,
                 "class": type(instances[name]).__name__}
                for name, status, realises in COMPONENTS]

    def stats(self) -> Dict[str, Any]:
        return {"gateway": self.gateway_id, "station": self.station,
                "samples_fed": self.samples_fed,
                "field_abstraction": self.field_abstraction.stats(),
                "data_routing": self.data_routing.totals(),
                "analytics": self.analytics.stats(),
                "automation": self.automation.stats(),
                "node_management": self.node_management.stats(),
                "ledger_clients": self.ledger_clients.stats(),
                "pdp": self.pdp.stats.as_dict(),
                "smart_object": self.plc.stats(),
                "protocol": self.adapter.stats()}
