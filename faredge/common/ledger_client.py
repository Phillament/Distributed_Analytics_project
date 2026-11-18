"""Ledger Clients Component -- present in the Field, Edge and Cloud Tiers.

Paper, S3.1 (Field Tier): "The Ledger Clients Component is a software library
that can be used by Smart Object logic to interact with Ledger Services in an
easy way.  It allows Smart Object logic to make outgoing calls to specific
Ledger Services.  The library contains a dedicated client for each Ledger
Service."

Paper, S3.2 (Edge Tier): "It provides the same functionality as the Component
with the same name in the Field Tier, but with the additional capability of
subscribing to ledger events."

Paper, S3.4 (Cloud Tier): "The Ledger Clients Component is used by the three
Open API Enablers to access Ledger Services."

That gives three concrete classes with a deliberate capability ladder:

    FieldLedgerClients  -- outgoing calls only
    EdgeLedgerClients   -- outgoing calls + event subscription
    CloudLedgerClients  -- outgoing calls + event subscription, over the WAN

and, inside each, one dedicated sub-client per Ledger Service, exactly as the
paper describes.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from .crypto import Identity
from .messages import LedgerEvent, Receipt, ServiceCall, TX_APPEND
from .network import FIELD_LAN, PLANT_LAN, WAN_CLOUD, Link


class _ServiceClient:
    """Dedicated client for one Ledger Service."""

    service: str = ""

    def __init__(self, owner: "FieldLedgerClients"):
        self._owner = owner

    def _call(self, endpoint: str, args: Dict[str, Any],
              op: str = TX_APPEND) -> Receipt:
        return self._owner.invoke(self.service, endpoint, args, op)

    def _read(self, key: str, default: Any = None) -> Any:
        return self._owner.read(self.service, key, default)


class OrchestrationClient(_ServiceClient):
    service = "OrchestrationServices"

    def define_route(self, route: List[str]) -> Receipt:
        return self._call("defineRoute", {"route": route})

    def open_work_order(self, wo_id: str, qty: int, recipe: str = "default") -> Receipt:
        return self._call("openWorkOrder", {"wo_id": wo_id, "qty": qty,
                                            "recipe": recipe})

    def acquire_station(self, station: str, eg: str) -> Receipt:
        return self._call("acquireStation", {"station": station, "eg": eg})

    def release_station(self, station: str, eg: str) -> Receipt:
        return self._call("releaseStation", {"station": station, "eg": eg})

    def claim_part(self, station: str, eg: str, wo_id: str = "") -> Receipt:
        return self._call("claimPart", {"station": station, "eg": eg,
                                        "wo_id": wo_id})

    def report_operation_complete(self, part_id: str, station: str, eg: str,
                                  cycle_time_s: float, ok: bool = True) -> Receipt:
        return self._call("reportOperationComplete",
                          {"part_id": part_id, "station": station, "eg": eg,
                           "cycle_time_s": cycle_time_s, "ok": ok})

    def close_work_order(self, wo_id: str) -> Receipt:
        return self._call("closeWorkOrder", {"wo_id": wo_id})

    def counters(self) -> Dict[str, Any]:
        return self._read("counters", {}) or {}

    def work_orders(self) -> Dict[str, Any]:
        return self._read("work_orders", {}) or {}


class ConfigurationClient(_ServiceClient):
    service = "ConfigurationServices"

    def register_asset(self, asset_id: str, **descriptor: Any) -> Receipt:
        return self._call("registerAsset", {"asset_id": asset_id, **descriptor})

    def deregister_asset(self, asset_id: str) -> Receipt:
        return self._call("deregisterAsset", {"asset_id": asset_id})

    def set_parameter(self, station: str, key: str, value: Any,
                      reason: str = "") -> Receipt:
        return self._call("setParameter", {"station": station, "key": key,
                                           "value": value, "reason": reason})

    def set_limit(self, key: str, low: float, high: float) -> Receipt:
        return self._call("setLimit", {"key": key, "min": low, "max": high})

    def publish_recipe(self, recipe_id: str, parameters: Dict[str, Any]) -> Receipt:
        return self._call("publishRecipe", {"recipe_id": recipe_id,
                                            "parameters": parameters})

    def apply_recipe(self, station: str, recipe_id: str) -> Receipt:
        return self._call("applyRecipe", {"station": station,
                                          "recipe_id": recipe_id})

    def registry(self) -> Dict[str, Any]:
        return self._read("registry", {}) or {}

    def parameters(self, station: str) -> Dict[str, Any]:
        return (self._read("parameters", {}) or {}).get(station, {})


class DataPublishingClient(_ServiceClient):
    service = "DataPublishingServices"

    def declare_kpi(self, kpi: str, method: str, unit: str = "",
                    description: str = "") -> Receipt:
        return self._call("declareKPI", {"kpi": kpi, "method": method,
                                         "unit": unit,
                                         "description": description})

    def submit_local_kpi(self, eg: str, kpi: str, value: float = 0.0,
                         weight: float = 1.0, numerator: Optional[float] = None,
                         denominator: Optional[float] = None) -> Receipt:
        args: Dict[str, Any] = {"eg": eg, "kpi": kpi}
        if numerator is not None and denominator is not None:
            args.update({"numerator": numerator, "denominator": denominator})
        else:
            args.update({"value": value, "weight": weight})
        return self._call("submitLocalKPI", args)

    def global_kpis(self) -> Dict[str, Any]:
        return self._read("global", {}) or {}

    def local_kpis(self) -> Dict[str, Any]:
        return self._read("local", {}) or {}


class SynchronizationClient(_ServiceClient):
    service = "SynchronizationServices"

    def register_model(self, model_id: str, params: Dict[str, float],
                       thresholds: Optional[Dict[str, float]] = None,
                       entity: str = "", kind: str = "MACHINE",
                       alpha: float = 0.5) -> Receipt:
        return self._call("registerModel",
                          {"model_id": model_id, "params": params,
                           "thresholds": thresholds or {}, "entity": entity,
                           "kind": kind, "alpha": alpha})

    def report_observation(self, model_id: str, param: str, observed: float,
                           n: int = 1) -> Receipt:
        return self._call("reportObservation",
                          {"model_id": model_id, "param": param,
                           "observed": observed, "n": n})

    def models(self) -> Dict[str, Any]:
        return self._read("models", {}) or {}


# --------------------------------------------------------------------------
# The Component itself, in its three Tier-specific flavours
# --------------------------------------------------------------------------
class FieldLedgerClients:
    """Field-Tier Ledger Clients: outgoing calls only."""

    link_class = FIELD_LAN
    can_subscribe = False
    tier = "FIELD"

    def __init__(self, owner_id: str, ledger, identity: Identity, link: Link):
        self.owner_id = owner_id
        self.identity = identity
        self._ledger = ledger
        self._link = link
        ledger.register_client(identity)

        # "The library contains a dedicated client for each Ledger Service."
        self.orchestration = OrchestrationClient(self)
        self.configuration = ConfigurationClient(self)
        self.data_publishing = DataPublishingClient(self)
        self.synchronization = SynchronizationClient(self)

        self.calls_made = 0
        self.reads_made = 0
        self.rejected = 0

    # -- outgoing calls --------------------------------------------------
    def invoke(self, service: str, endpoint: str, args: Dict[str, Any],
               op: str = TX_APPEND) -> Receipt:
        call = ServiceCall(service=service, endpoint=endpoint, args=args,
                           caller=self.identity.address, op=op)
        self._link.transmit(call.wire_size(), "SERVICE_CALL")
        self.calls_made += 1
        receipt = self._ledger.submit(call, self.identity)
        self._link.transmit(160, "RECEIPT")
        if not receipt.accepted:
            self.rejected += 1
        return receipt

    # -- reads (no consensus, so they are cheap and frequent) ------------
    def read(self, service: str, key: str, default: Any = None) -> Any:
        self._link.transmit(96, "LEDGER_READ")
        self.reads_made += 1
        value = self._ledger.read(service, key, default)
        return value

    def stats(self) -> Dict[str, int]:
        return {"calls": self.calls_made, "reads": self.reads_made,
                "rejected": self.rejected}


class EdgeLedgerClients(FieldLedgerClients):
    """Edge-Tier Ledger Clients: calls plus ledger-event subscription."""

    link_class = PLANT_LAN
    can_subscribe = True
    tier = "EDGE"

    def __init__(self, owner_id: str, ledger, identity: Identity, link: Link):
        super().__init__(owner_id, ledger, identity, link)
        self._handlers: Dict[str, List[Callable[[LedgerEvent], None]]] = {}
        self.events_received = 0
        ledger.subscribe("", self._on_event)

    def on(self, event_name: str,
           handler: Callable[[LedgerEvent], None]) -> None:
        """Subscribe to a ledger event by name ("*" for every event)."""
        self._handlers.setdefault(event_name, []).append(handler)

    def _on_event(self, event: LedgerEvent) -> None:
        handlers = self._handlers.get(event.name, []) + self._handlers.get("*", [])
        if not handlers:
            return
        self._link.transmit(event.wire_size(), "LEDGER_EVENT")
        self.events_received += 1
        for handler in handlers:
            handler(event)


class CloudLedgerClients(EdgeLedgerClients):
    """Cloud-Tier Ledger Clients, used by the three Open API Enablers."""

    link_class = WAN_CLOUD
    tier = "CLOUD"
