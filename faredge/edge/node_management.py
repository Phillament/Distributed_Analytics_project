"""Node Management Component (Edge Tier).

Paper, S3.2: "The Node Management Component implements some technical hooks --
e.g., service and/or subscription endpoints -- that allow for the remote
management of EGs.  This functionality implements the Management XC Functions
at the Edge level and is entirely implementation-dependent."

Paper, S2.2 (Management XC): "Low-level functions for monitoring and
commissioning/decommissioning of individual system modules."

The implementation exposes exactly those hooks: a set of named management
endpoints invocable remotely (by the Cloud Tier's Platform Management
Component), a heartbeat, and a subscription mechanism for pushing node status
upwards.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

COMMISSIONED = "COMMISSIONED"
DECOMMISSIONED = "DECOMMISSIONED"
DEGRADED = "DEGRADED"


@dataclass
class NodeStatus:
    node_id: str
    state: str = COMMISSIONED
    uptime_s: float = 0.0
    heartbeats: int = 0
    last_heartbeat_t: float = 0.0
    metrics: Dict[str, Any] = field(default_factory=dict)


class NodeManagement:
    """Remote-management hooks of one Edge Gateway."""

    def __init__(self, gateway_id: str,
                 metrics_provider: Optional[Callable[[], Dict[str, Any]]] = None):
        self.gateway_id = gateway_id
        self.status = NodeStatus(node_id=gateway_id)
        self._metrics_provider = metrics_provider
        self._started = time.time()
        self._subscribers: List[Callable[[NodeStatus], None]] = []
        self._endpoints: Dict[str, Callable[..., Dict[str, Any]]] = {
            "getStatus": self.get_status,
            "heartbeat": self.heartbeat,
            "commission": self.commission,
            "decommission": self.decommission,
            "getMetrics": self.get_metrics,
        }
        self.invocations = 0

    # -- service endpoints -----------------------------------------------
    def endpoints(self) -> List[str]:
        return sorted(self._endpoints)

    def invoke(self, endpoint: str, **kwargs: Any) -> Dict[str, Any]:
        """Remote management call, as made by Platform Management."""
        handler = self._endpoints.get(endpoint)
        if handler is None:
            return {"ok": False, "reason": f"unknown endpoint {endpoint}"}
        self.invocations += 1
        return handler(**kwargs)

    def register_endpoint(self, name: str,
                          handler: Callable[..., Dict[str, Any]]) -> None:
        self._endpoints[name] = handler

    # -- subscription endpoint -------------------------------------------
    def subscribe(self, callback: Callable[[NodeStatus], None]) -> None:
        self._subscribers.append(callback)

    def _notify(self) -> None:
        for callback in self._subscribers:
            callback(self.status)

    # -- monitoring ------------------------------------------------------
    def heartbeat(self) -> Dict[str, Any]:
        self.status.heartbeats += 1
        self.status.last_heartbeat_t = time.time()
        self.status.uptime_s = self.status.last_heartbeat_t - self._started
        self._notify()
        return {"ok": True, "node": self.gateway_id,
                "heartbeats": self.status.heartbeats,
                "uptime_s": round(self.status.uptime_s, 3)}

    def get_status(self) -> Dict[str, Any]:
        return {"ok": True, "node": self.gateway_id, "state": self.status.state,
                "uptime_s": round(time.time() - self._started, 3),
                "heartbeats": self.status.heartbeats}

    def get_metrics(self) -> Dict[str, Any]:
        metrics = self._metrics_provider() if self._metrics_provider else {}
        self.status.metrics = metrics
        return {"ok": True, "node": self.gateway_id, "metrics": metrics}

    # -- commissioning ---------------------------------------------------
    def commission(self) -> Dict[str, Any]:
        self.status.state = COMMISSIONED
        self._notify()
        return {"ok": True, "node": self.gateway_id, "state": self.status.state}

    def decommission(self) -> Dict[str, Any]:
        self.status.state = DECOMMISSIONED
        self._notify()
        return {"ok": True, "node": self.gateway_id, "state": self.status.state}

    def mark_degraded(self, reason: str = "") -> Dict[str, Any]:
        self.status.state = DEGRADED
        self.status.metrics["degraded_reason"] = reason
        self._notify()
        return {"ok": True, "node": self.gateway_id, "state": DEGRADED,
                "reason": reason}

    @property
    def is_operational(self) -> bool:
        return self.status.state != DECOMMISSIONED

    def stats(self) -> Dict[str, Any]:
        return {"node": self.gateway_id, "state": self.status.state,
                "heartbeats": self.status.heartbeats,
                "management_invocations": self.invocations,
                "endpoints": self.endpoints()}
