"""The FAR-EDGE Platform: all four Tiers wired together.

This is the executable form of Figure 2 of the paper ("FAR-EDGE Platform
Design").  It instantiates:

    Field Tier   -- Smart Objects and Connected Devices per station, wrapping
                    Things, People and Environments
    Edge Tier    -- one Edge Gateway per station, with all seven Components
    Ledger Tier  -- the Distributed Ledger Enabler on N Peer Nodes, hosting
                    the four Ledger Services Components
    Cloud Tier   -- a Cloud Server with the eleven Cloud-Tier Components

and provides the operations an integrator would actually perform: bootstrap
the plant (policies, identities, route, work order, KPI declarations), run a
production shift against field data, and read the results back out through the
Open APIs.
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence

from .cloud.cloud_tier import CloudTier
from .common.config_bridge import set_network_config
from .common.messages import TX_APPEND
from .common.network import (FIELD_LAN, MONITOR, PEER_P2P, PLANT_LAN,
                             WAN_CLOUD, build_links)
from .common.pdp import PERMIT
from .config import Domain, PlatformConfig, Scope, Tier, XC
from .edge.gateway import EdgeGateway, StationProfile
from .ledger.dlt import DistributedLedger

# Global KPIs declared at bootstrap, with the aggregation rule each one uses.
GLOBAL_KPIS = (
    ("plant_parts_good", "sum", "parts"),
    ("plant_energy_kwh", "sum", "kWh"),
    ("line_throughput_pph", "min", "parts/h"),
    ("plant_availability", "weighted_mean", "ratio"),
    ("plant_oee", "weighted_mean", "ratio"),
    ("plant_mean_cycle_time_s", "weighted_mean", "s"),
    ("plant_quality", "ratio", "ratio"),
    ("plant_energy_per_part_kwh", "ratio", "kWh/part"),
    ("plant_peak_vibration_mm_s", "max", "mm/s"),
    ("plant_anomalies", "sum", "count"),
)

# Subjects enrolled in Identity Management at bootstrap.
SUBJECTS = (
    ("line_engineer", ["engineer", "operator"]),
    ("external_mes", ["integration"]),
    ("plant_viewer", ["viewer"]),
    ("guest", []),                 # deliberately given no policy: default deny
)


@dataclass
class RunReport:
    samples_processed: int = 0
    wall_time_s: float = 0.0
    kpi_publications: int = 0
    model_reports: int = 0
    gateways: int = 0
    ledger: Dict[str, Any] = field(default_factory=dict)
    network: Dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        return {"samples_processed": self.samples_processed,
                "wall_time_s": round(self.wall_time_s, 3),
                "kpi_publications": self.kpi_publications,
                "model_reports": self.model_reports,
                "gateways": self.gateways,
                "ledger": self.ledger, "network": self.network}


class FarEdgePlatform:
    """A complete FAR-EDGE deployment."""

    def __init__(self, cfg: Optional[PlatformConfig] = None,
                 station_profiles: Optional[Sequence[StationProfile]] = None,
                 logger: Optional[Callable[[str], None]] = None):
        self.cfg = cfg or PlatformConfig()
        self._logger = logger or (lambda m: None)
        set_network_config(self.cfg.network)
        self.links = build_links(self.cfg.network)

        self.profiles: List[StationProfile] = list(
            station_profiles or default_station_profiles(self.cfg.n_stations))
        self.stations = [p.station for p in self.profiles]

        # ---- Ledger Tier -----------------------------------------------
        # Peer Nodes are spread across Tiers on purpose: the paper stresses
        # that the Ledger Tier is logical and "does not mandate any specific
        # deployment choice for Peer Nodes".
        host_tiers = [Tier.EDGE if i % 2 == 0 else Tier.CLOUD
                      for i in range(self.cfg.ledger.n_peer_nodes)]
        self.ledger = DistributedLedger(self.cfg.ledger, self.links[PEER_P2P],
                                        host_tiers)
        self.ledger.start()

        # ---- Cloud Tier ------------------------------------------------
        self.cloud = CloudTier(self.ledger, self.links[WAN_CLOUD],
                               self.stations, self.cfg.ledger.rsa_key_bits)

        # ---- Edge Tier (and, through it, the Field Tier) ---------------
        self.gateways: Dict[str, EdgeGateway] = {}
        for profile in self.profiles:
            gateway = EdgeGateway(
                gateway_id=f"EG-{profile.station}", profile=profile,
                cfg=self.cfg, ledger=self.ledger, links=self.links,
                pap=self.cloud.security_management.pap,
                cloud_sink=self.cloud.ingest_stream, logger=self._log)
            self.gateways[gateway.gateway_id] = gateway
            self.cloud.attach_gateway(gateway)

        self.work_order = ""
        self.bootstrapped = False

    # ------------------------------------------------------------------
    def _log(self, message: str) -> None:
        if self.cfg.verbose:
            self._logger(message)

    # ------------------------------------------------------------------
    # bootstrap
    # ------------------------------------------------------------------
    def bootstrap(self, work_order: str = "WO-1000", qty: int = 10_000) -> None:
        """Bring the plant into a state where production can start."""
        self.work_order = work_order

        # 1. Security XC: administer access policies centrally.
        sm = self.cloud.security_management
        for profile in self.profiles:
            station = profile.station
            sm.grant(f"edge-control-{station}",
                     subjects=[f"EG-{station}", f"{station}-PLC"],
                     actions=["start", "stop", "set_speed", "reconfigure"],
                     resources=[f"{station}/*"])
        sm.grant("api-engineer", subjects=["line_engineer"], actions=["*"],
                 resources=["OpenAPI/Automation/*", "OpenAPI/Analytics/*",
                            "OpenAPI/Virtualization/*"])
        sm.grant("api-integration", subjects=["external_mes"],
                 actions=["getWorkOrders", "getGlobalKPIs", "getLineState",
                          "getAssetRegistry", "openWorkOrder", "getModel",
                          "listModels", "runSimulation", "runWhatIf"],
                 resources=["OpenAPI/Automation/*", "OpenAPI/Analytics/*",
                            "OpenAPI/Virtualization/*"])
        sm.grant("api-viewer", subjects=["plant_viewer"],
                 actions=["getGlobalKPIs", "getStationKPIs", "getLineState"],
                 resources=["OpenAPI/Analytics/*", "OpenAPI/Automation/*"])

        # 2. Security XC: digital identities and single sign-on.
        for subject, roles in SUBJECTS:
            self.cloud.identity_management.enrol(subject, roles)

        # 3. Automation Domain: the global route and the work order.
        cloud_ledger = self.cloud.ledger_clients
        cloud_ledger.orchestration.define_route(self.stations)
        cloud_ledger.orchestration.open_work_order(work_order, qty)

        # 4. Automation Domain: the shared safety envelope for parameters.
        cloud_ledger.configuration.set_limit("speed_factor", 0.2, 1.2)

        # 5. Analytics Domain: declare the global KPIs and how they aggregate.
        for kpi, method, unit in GLOBAL_KPIS:
            cloud_ledger.data_publishing.declare_kpi(kpi, method, unit)

        # 6. Commission every Edge Gateway: plug-and-play registration of its
        #    Field devices, digital model publication, workflow deployment.
        for index, gateway in enumerate(self.gateways.values()):
            gateway.commission(work_order, is_first_station=(index == 0))

        # 7. Mirror the as-designed models into the Model Repository.
        for profile in self.profiles:
            self.cloud.model_repository.put(
                profile.station,
                {"nominal_power_kw": profile.nominal_power_kw,
                 "ideal_cycle_time_s": profile.ideal_cycle_time_s},
                version=1)

        self.bootstrapped = True
        self._log(f"platform bootstrapped: {len(self.gateways)} gateways, "
                  f"{self.cfg.ledger.n_peer_nodes} peer nodes")

    @property
    def as_designed_models(self) -> Dict[str, Dict[str, float]]:
        return {p.station: {"nominal_power_kw": p.nominal_power_kw,
                            "ideal_cycle_time_s": p.ideal_cycle_time_s}
                for p in self.profiles}

    # ------------------------------------------------------------------
    # production run
    # ------------------------------------------------------------------
    def run_shift(self, rows_by_station: Dict[str, List[Dict[str, float]]],
                  publish_every: Optional[int] = None,
                  concurrent: bool = True) -> RunReport:
        """Feed field data through the platform.

        Each Edge Gateway processes its own station's data; with
        `concurrent=True` every gateway runs in its own thread, so the Ledger
        Tier really does serve parallel clients and the consensus protocol
        really is exercised concurrently.
        """
        if not self.bootstrapped:
            self.bootstrap()
        publish_every = publish_every or (self.cfg.edge.analytics_window * 20)
        started = time.perf_counter()
        totals = {"samples": 0, "publications": 0}
        lock = threading.Lock()

        def drive(gateway: EdgeGateway) -> None:
            rows = rows_by_station.get(gateway.station, [])
            published = 0
            for index, row in enumerate(rows, start=1):
                gateway.feed(row)
                if index % publish_every == 0:
                    published += gateway.publish_kpis()
            with lock:
                totals["samples"] += len(rows)
                totals["publications"] += published

        if concurrent:
            threads = [threading.Thread(target=drive, args=(eg,), daemon=True,
                                        name=eg.gateway_id)
                       for eg in self.gateways.values()]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
        else:
            for gateway in self.gateways.values():
                drive(gateway)

        # Final flush: publish every KPI unconditionally so that the global
        # view is exact at the end of the shift, and synchronise the models.
        publications = totals["publications"]
        model_reports = 0
        for gateway in self.gateways.values():
            publications += gateway.publish_kpis(force=True)
            model_reports += gateway.synchronise_model()

        self.ledger.stop()
        report = RunReport(
            samples_processed=totals["samples"],
            wall_time_s=time.perf_counter() - started,
            kpi_publications=publications, model_reports=model_reports,
            gateways=len(self.gateways),
            ledger=self.ledger.stats.as_dict(),
            network=MONITOR.summary())
        self.ledger.start()
        return report

    # ------------------------------------------------------------------
    # teardown and introspection
    # ------------------------------------------------------------------
    def shutdown(self) -> None:
        for gateway in self.gateways.values():
            gateway.decommission()
        if self.work_order:
            self.cloud.ledger_clients.orchestration.close_work_order(self.work_order)
        self.ledger.stop()

    def global_kpis(self) -> Dict[str, Any]:
        return self.cloud.ledger_clients.data_publishing.global_kpis()

    def station_kpis(self) -> Dict[str, Dict[str, float]]:
        return {eg.station: eg.analytics.local_kpis()
                for eg in self.gateways.values()}

    def tier_inventory(self) -> Dict[str, Any]:
        """Structural self-description, used by the conformance experiment."""
        first = next(iter(self.gateways.values()))
        return {
            Tier.FIELD: {
                "scope": Scope.PLANT,
                "edge_nodes": sum(len(eg.field_nodes) for eg in self.gateways.values()),
                "smart_objects": sum(
                    1 for eg in self.gateways.values()
                    for n in eg.field_nodes.values() if n.has_onboard_compute),
                "connected_devices": sum(
                    1 for eg in self.gateways.values()
                    for n in eg.field_nodes.values() if not n.has_onboard_compute),
                "real_world_entities": [e.entity_class for eg in self.gateways.values()
                                        for e in eg.entities],
                "components": ["Policy Decision Point", "Ledger Clients"],
                "protocols": sorted({eg.adapter.name for eg in self.gateways.values()}),
            },
            Tier.EDGE: {
                "scope": Scope.PLANT,
                "gateways": len(self.gateways),
                "components": first.component_inventory(),
            },
            Tier.LEDGER: {
                "scope": f"{Scope.PLANT}+{Scope.ENTERPRISE}",
                "peer_nodes": len(self.ledger.peers),
                "host_tiers": sorted({p.host_tier for p in self.ledger.peers}),
                "services": self.ledger.installed_services(),
                "enabler": "Distributed Ledger Enabler",
            },
            Tier.CLOUD: {
                "scope": Scope.ENTERPRISE,
                "components": self.cloud.component_inventory(),
            },
        }

    def stats(self) -> Dict[str, Any]:
        return {"config": {"stations": self.cfg.n_stations,
                           "gateways": self.cfg.n_gateways,
                           "peer_nodes": self.cfg.ledger.n_peer_nodes,
                           "protocol": self.cfg.field_tier.protocol},
                "ledger": self.ledger.stats.as_dict(),
                "replicas": self.ledger.verify_replicas(),
                "cloud": self.cloud.stats(),
                "gateways": {gid: eg.stats() for gid, eg in self.gateways.items()},
                "network": MONITOR.summary()}


# --------------------------------------------------------------------------
# default plant layout
# --------------------------------------------------------------------------
def default_station_profiles(n_stations: int) -> List[StationProfile]:
    """A four-station flow line with station 3 as the designed bottleneck.

    The profile values are the *as-designed* machine data; the dataset
    generator makes the real machines deviate from them, which is what gives
    the Digital Models XC Functions something to correct.
    """
    base = [
        StationProfile("ST1", ideal_cycle_time_s=10.0, nominal_power_kw=4.0,
                       vibration_warn=4.5, vibration_trip=9.0),
        StationProfile("ST2", ideal_cycle_time_s=11.0, nominal_power_kw=6.5,
                       vibration_warn=5.0, vibration_trip=9.5),
        StationProfile("ST3", ideal_cycle_time_s=12.5, nominal_power_kw=7.5,
                       vibration_warn=5.5, vibration_trip=10.0),
        StationProfile("ST4", ideal_cycle_time_s=9.5, nominal_power_kw=3.5,
                       vibration_warn=4.0, vibration_trip=8.5),
    ]
    if n_stations <= len(base):
        return base[:n_stations]
    profiles = list(base)
    for i in range(len(base), n_stations):
        template = base[i % len(base)]
        profiles.append(StationProfile(
            f"ST{i + 1}",
            ideal_cycle_time_s=template.ideal_cycle_time_s,
            nominal_power_kw=template.nominal_power_kw,
            vibration_warn=template.vibration_warn,
            vibration_trip=template.vibration_trip))
    return profiles
