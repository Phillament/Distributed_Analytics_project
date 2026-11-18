"""The three Open API Enablers (Cloud Tier, Public Cloud Services).

Paper, S3.4:

  * "The Open API for Automation Enabler implements all the public cloud
     service endpoints belonging to the FAR-EDGE Automation Domain, accessed
     over the Web through HTTPS.  It enables access to Edge Automation
     Services, Orchestration Services and Configuration Services."

  * "The Open API for Analytics Enabler implements all the public cloud
     service endpoints belonging to the Analytics Domain.  These endpoints
     allow external systems to interact with the distributed data analysis
     logic deployed on the Edge Tier (Edge Analytics Engine) and on the Ledger
     Tier (Data Publishing Services)."

  * "The Open API for Virtualization Enabler implements the public cloud
     service endpoints, belonging to the Simulation Domain, related to the
     integration of external simulation tools.  It also implements the public
     cloud service endpoints belonging to the Digital Models XC Functions."

Every request goes through the same three steps, which is how the Cloud Tier's
own Components hang together:

    1. Identity Management resolves the caller's single-sign-on token,
    2. the Cloud-Tier Policy Decision Point authorises the (action, resource),
    3. the call is served through Cloud Ledger Clients and/or the Edge Tier.

Request and response sizes are charged to the WAN link, so the cost of
reaching the platform from outside the plant is visible in the measurements.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from ..common.crypto import canonical_json
from ..common.network import WAN_CLOUD, Link
from ..config import Domain, XC


@dataclass
class ApiResponse:
    ok: bool
    data: Dict[str, Any] = field(default_factory=dict)
    status: int = 200
    reason: str = ""

    def wire_size(self) -> int:
        return len(canonical_json({"ok": self.ok, "data": self.data,
                                   "status": self.status}).encode("utf-8"))


class _OpenApiBase:
    """Shared HTTPS front end: authentication, authorisation, accounting."""

    name = "Open API"
    realises = ""

    def __init__(self, idm, pdp, ledger_clients, wan: Link):
        self._idm = idm
        self._pdp = pdp
        self.ledger = ledger_clients
        self._wan = wan
        self.requests = 0
        self.unauthorised = 0
        self.endpoints: Dict[str, Callable[..., ApiResponse]] = {}

    # -- plumbing --------------------------------------------------------
    def call(self, endpoint: str, token: str = "", **kwargs: Any) -> ApiResponse:
        """Serve one HTTPS request."""
        self.requests += 1
        self._wan.transmit(len(canonical_json(kwargs).encode("utf-8")) + 320,
                           "API_REQUEST")
        handler = self.endpoints.get(endpoint)
        if handler is None:
            return self._respond(ApiResponse(False, status=404,
                                             reason=f"no endpoint {endpoint}"))

        session = self._idm.resolve(token) if token else None
        subject = session.subject if session else "anonymous"
        resource = f"{self.name}/{endpoint}"
        decision = self._pdp.evaluate(subject, endpoint, resource)
        if not decision.permitted:
            self.unauthorised += 1
            return self._respond(ApiResponse(
                False, status=401 if session is None else 403,
                reason=f"not authorised: {decision.reason}"))
        return self._respond(handler(**kwargs))

    def _respond(self, response: ApiResponse) -> ApiResponse:
        self._wan.transmit(response.wire_size(), "API_RESPONSE")
        return response

    def list_endpoints(self) -> List[str]:
        return sorted(self.endpoints)

    def stats(self) -> Dict[str, Any]:
        return {"api": self.name, "requests": self.requests,
                "unauthorised": self.unauthorised,
                "endpoints": self.list_endpoints()}


class OpenApiForAutomation(_OpenApiBase):
    """Public endpoints of the Automation Domain."""

    name = "OpenAPI/Automation"
    realises = Domain.AUTOMATION
    status = "ENABLER"

    def __init__(self, idm, pdp, ledger_clients, wan: Link,
                 gateways: Optional[Dict[str, Any]] = None):
        super().__init__(idm, pdp, ledger_clients, wan)
        self._gateways = gateways if gateways is not None else {}
        self.endpoints = {
            # -> Orchestration Services (Ledger Tier)
            "getWorkOrders": self.get_work_orders,
            "openWorkOrder": self.open_work_order,
            "closeWorkOrder": self.close_work_order,
            "getLineState": self.get_line_state,
            # -> Configuration Services (Ledger Tier)
            "getAssetRegistry": self.get_asset_registry,
            "setStationParameter": self.set_station_parameter,
            "applyRecipe": self.apply_recipe,
            # -> Edge Automation Services (Edge Tier)
            "getStationWorkflows": self.get_station_workflows,
        }

    def get_work_orders(self) -> ApiResponse:
        return ApiResponse(True, {"work_orders": self.ledger.orchestration.work_orders()})

    def open_work_order(self, wo_id: str, qty: int,
                        recipe: str = "default") -> ApiResponse:
        receipt = self.ledger.orchestration.open_work_order(wo_id, qty, recipe)
        return ApiResponse(receipt.accepted, {"receipt": receipt.__dict__},
                           200 if receipt.accepted else 409, receipt.reason)

    def close_work_order(self, wo_id: str) -> ApiResponse:
        receipt = self.ledger.orchestration.close_work_order(wo_id)
        return ApiResponse(receipt.accepted, {"receipt": receipt.__dict__},
                           200 if receipt.accepted else 409, receipt.reason)

    def get_line_state(self) -> ApiResponse:
        return ApiResponse(True, {"counters": self.ledger.orchestration.counters(),
                                  "locks": self.ledger.read("OrchestrationServices",
                                                            "locks", {})})

    def get_asset_registry(self) -> ApiResponse:
        return ApiResponse(True, {"registry": self.ledger.configuration.registry()})

    def set_station_parameter(self, station: str, key: str, value: Any,
                              reason: str = "") -> ApiResponse:
        receipt = self.ledger.configuration.set_parameter(station, key, value, reason)
        return ApiResponse(receipt.accepted, {"receipt": receipt.__dict__},
                           200 if receipt.accepted else 409, receipt.reason)

    def apply_recipe(self, station: str, recipe_id: str) -> ApiResponse:
        receipt = self.ledger.configuration.apply_recipe(station, recipe_id)
        return ApiResponse(receipt.accepted, {"receipt": receipt.__dict__},
                           200 if receipt.accepted else 409, receipt.reason)

    def get_station_workflows(self, gateway: str = "") -> ApiResponse:
        if gateway and gateway in self._gateways:
            return ApiResponse(True, {gateway: self._gateways[gateway].automation.stats()})
        return ApiResponse(True, {gid: eg.automation.stats()
                                  for gid, eg in self._gateways.items()})


class OpenApiForAnalytics(_OpenApiBase):
    """Public endpoints of the Analytics Domain."""

    name = "OpenAPI/Analytics"
    realises = Domain.ANALYTICS
    status = "ENABLER"

    def __init__(self, idm, pdp, ledger_clients, wan: Link,
                 gateways: Optional[Dict[str, Any]] = None,
                 data_storage: Optional[Any] = None):
        super().__init__(idm, pdp, ledger_clients, wan)
        self._gateways = gateways if gateways is not None else {}
        self._storage = data_storage
        self.endpoints = {
            # -> Data Publishing Services (Ledger Tier)
            "getGlobalKPIs": self.get_global_kpis,
            "getLocalKPIs": self.get_local_kpis,
            "declareKPI": self.declare_kpi,
            # -> Edge Analytics Engine (Edge Tier)
            "getStationKPIs": self.get_station_kpis,
            "getEngineStatus": self.get_engine_status,
            # -> Data Storage (Internal Cloud Service)
            "getStoredVolume": self.get_stored_volume,
        }

    def get_global_kpis(self) -> ApiResponse:
        return ApiResponse(True, {"global": self.ledger.data_publishing.global_kpis()})

    def get_local_kpis(self) -> ApiResponse:
        return ApiResponse(True, {"local": self.ledger.data_publishing.local_kpis()})

    def declare_kpi(self, kpi: str, method: str, unit: str = "") -> ApiResponse:
        receipt = self.ledger.data_publishing.declare_kpi(kpi, method, unit)
        return ApiResponse(receipt.accepted, {"receipt": receipt.__dict__},
                           200 if receipt.accepted else 409, receipt.reason)

    def get_station_kpis(self, gateway: str = "") -> ApiResponse:
        if gateway and gateway in self._gateways:
            return ApiResponse(True, {gateway: self._gateways[gateway].analytics.local_kpis()})
        return ApiResponse(True, {gid: eg.analytics.local_kpis()
                                  for gid, eg in self._gateways.items()})

    def get_engine_status(self) -> ApiResponse:
        return ApiResponse(True, {gid: eg.analytics.stats()
                                  for gid, eg in self._gateways.items()})

    def get_stored_volume(self) -> ApiResponse:
        if self._storage is None:
            return ApiResponse(False, status=503, reason="no Data Storage bound")
        return ApiResponse(True, self._storage.stats())


class OpenApiForVirtualization(_OpenApiBase):
    """Public endpoints of the Simulation Domain and Digital Models XC."""

    name = "OpenAPI/Virtualization"
    realises = Domain.SIMULATION
    status = "ENABLER"

    def __init__(self, idm, pdp, ledger_clients, wan: Link,
                 repository: Optional[Any] = None,
                 simulator: Optional[Any] = None):
        super().__init__(idm, pdp, ledger_clients, wan)
        self._repository = repository
        self._simulator = simulator
        self.endpoints = {
            # Digital Models XC Functions
            "getModel": self.get_model,
            "listModels": self.list_models,
            "putModel": self.put_model,
            "getModelHistory": self.get_model_history,
            # Simulation Domain: integration of external simulation tools
            "runSimulation": self.run_simulation,
            "runWhatIf": self.run_what_if,
        }

    # -- Digital Models --------------------------------------------------
    def get_model(self, model_id: str) -> ApiResponse:
        ledger_model = self.ledger.synchronization.models().get(model_id)
        stored = self._repository.get(model_id) if self._repository else None
        if ledger_model is None and stored is None:
            return ApiResponse(False, status=404, reason=f"no model {model_id}")
        return ApiResponse(True, {"ledger": ledger_model, "repository": stored})

    def list_models(self) -> ApiResponse:
        return ApiResponse(True, {"models": sorted(self.ledger.synchronization.models())})

    def put_model(self, model_id: str, payload: Dict[str, Any],
                  version: int = 1) -> ApiResponse:
        if self._repository is None:
            return ApiResponse(False, status=503, reason="no Model Repository bound")
        return ApiResponse(True, {"model": self._repository.put(model_id, payload, version)})

    def get_model_history(self, model_id: str) -> ApiResponse:
        if self._repository is None:
            return ApiResponse(False, status=503, reason="no Model Repository bound")
        return ApiResponse(True, {"history": self._repository.history(model_id)})

    # -- Simulation ------------------------------------------------------
    def run_simulation(self, duration_s: float = 3600.0,
                       use_synced_models: bool = True) -> ApiResponse:
        if self._simulator is None:
            return ApiResponse(False, status=503, reason="no simulator bound")
        result = self._simulator.run(duration_s=duration_s,
                                     use_synced_models=use_synced_models)
        return ApiResponse(True, result)

    def run_what_if(self, overrides: Dict[str, Dict[str, float]],
                    duration_s: float = 3600.0) -> ApiResponse:
        """Test a what/if scenario "at minimal cost and without essential
        impact on regular shop activities" (paper S2.2)."""
        if self._simulator is None:
            return ApiResponse(False, status=503, reason="no simulator bound")
        return ApiResponse(True, self._simulator.what_if(overrides, duration_s))
