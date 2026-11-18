"""Internal Cloud Services and Platform Cloud Tools (Cloud Tier).

Paper, S3.4: "The Cloud Tier includes a total of eleven components, three of
them having Enabler status.  They are classified in: (i) Public Cloud
Services, which are accessible through three Open APIs ...; (ii) Internal
Cloud Services, including Identity Management, Model Repository and Data
Storage; and (iii) Platform Cloud Tools, including security Management, PDP,
Ledger Clients, Real-to-Digital Synchronization and Platform Management
components."

This module implements groups (ii) and (iii) except the PDP and the Ledger
Clients, which are shared with the other Tiers and live in `faredge.common`.
The three Open API Enablers of group (i) live in `open_api.py`.
"""
from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from ..common.crypto import Identity, KeyDirectory, sha256_hex
from ..common.messages import StreamRecord
from ..common.pdp import (DENY, PERMIT, Policy, PolicyAdministrationPoint,
                          PolicyDecisionPoint)
from ..config import Scope, Tier, XC


# --------------------------------------------------------------------------
# (ii) Internal Cloud Services
# --------------------------------------------------------------------------
@dataclass
class Session:
    token: str
    subject: str
    roles: List[str]
    issued_t: float
    ttl_s: float = 3600.0

    def valid(self, now: Optional[float] = None) -> bool:
        return (now or time.time()) - self.issued_t < self.ttl_s


class IdentityManagement:
    """Identity Management (IDM) Component.

    Paper: "implements the Security XC Functions that relate to digital
    identities.  It is in charge of exposing single-sign-on service endpoints
    for other Platform services."
    """

    component = "Identity Management"
    realises = XC.SECURITY

    def __init__(self) -> None:
        self._subjects: Dict[str, Dict[str, Any]] = {}
        self._sessions: Dict[str, Session] = {}
        self.directory = KeyDirectory()
        self.logins = 0
        self.rejected_logins = 0

    def enrol(self, subject: str, roles: List[str],
              secret: str = "", identity: Optional[Identity] = None) -> str:
        self._subjects[subject] = {"roles": roles,
                                   "secret_hash": sha256_hex(secret or subject)}
        if identity is not None:
            self.directory.register(identity)
        return subject

    def single_sign_on(self, subject: str, secret: str = "") -> Optional[Session]:
        """The SSO endpoint used by every other Platform service."""
        record = self._subjects.get(subject)
        if not record or record["secret_hash"] != sha256_hex(secret or subject):
            self.rejected_logins += 1
            return None
        token = sha256_hex(f"{subject}:{time.time()}")[:32]
        session = Session(token=token, subject=subject, roles=record["roles"],
                          issued_t=time.time())
        self._sessions[token] = session
        self.logins += 1
        return session

    def resolve(self, token: str) -> Optional[Session]:
        session = self._sessions.get(token)
        return session if session and session.valid() else None

    def roles_of(self, token: str) -> List[str]:
        session = self.resolve(token)
        return list(session.roles) if session else []

    def stats(self) -> Dict[str, Any]:
        return {"subjects": len(self._subjects), "sessions": len(self._sessions),
                "logins": self.logins, "rejected_logins": self.rejected_logins}


class ModelRepository:
    """Model Repository Component.

    Paper: "implements those low-level parts of the Digital Models XC
    Functions that are not exposed to external systems."  The authoritative,
    *shared* model lives on the ledger (Synchronization Services); this
    repository holds the heavy, non-shared artefacts -- geometry, process
    plans, simulation meshes -- plus the version history.
    """

    component = "Model Repository"
    realises = XC.DIGITAL_MODELS

    def __init__(self) -> None:
        self._models: Dict[str, Dict[str, Any]] = {}
        self._history: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    def put(self, model_id: str, payload: Dict[str, Any],
            version: int = 1) -> Dict[str, Any]:
        record = {"model_id": model_id, "version": version,
                  "payload": payload, "t": time.time()}
        self._models[model_id] = record
        self._history[model_id].append(record)
        return record

    def get(self, model_id: str) -> Optional[Dict[str, Any]]:
        return self._models.get(model_id)

    def history(self, model_id: str) -> List[Dict[str, Any]]:
        return list(self._history[model_id])

    def ids(self) -> List[str]:
        return sorted(self._models)

    def stats(self) -> Dict[str, Any]:
        return {"models": len(self._models),
                "versions": sum(len(v) for v in self._history.values())}


class DataStorage:
    """Data Storage Component.

    Paper: "implements generic persistence for the purpose of internal sharing
    of data."  This is where the filtered, pre-processed cloud-bound stream
    lands; its byte counter is what the bandwidth experiment compares against
    the unfiltered baseline.
    """

    component = "Data Storage"
    realises = "INTERNAL"

    def __init__(self) -> None:
        self.records: List[StreamRecord] = []
        self.bytes_stored = 0
        self._keep_records = True
        self.series: Dict[str, List[float]] = defaultdict(list)

    def store(self, record: StreamRecord) -> None:
        self.bytes_stored += record.wire_size()
        if self._keep_records:
            self.records.append(record)
        for signal, value in record.values.items():
            self.series[f"{record.source}/{signal}"].append(value)

    def set_retention(self, keep_records: bool) -> None:
        """Large-scale runs can switch off record retention to stay in RAM."""
        self._keep_records = keep_records

    def count(self) -> int:
        return len(self.records)

    def stats(self) -> Dict[str, Any]:
        return {"records": len(self.records), "bytes_stored": self.bytes_stored,
                "series": len(self.series)}


# --------------------------------------------------------------------------
# (iii) Platform Cloud Tools
# --------------------------------------------------------------------------
class SecurityManagement:
    """Security Management Component.

    Paper: "implements those parts of the Security XC Functions related to the
    administration of access policies."  It *is* the Policy Administration
    Point that every Tier's PDP consults.
    """

    component = "Security Management"
    realises = XC.SECURITY

    def __init__(self) -> None:
        self.pap = PolicyAdministrationPoint()

    def grant(self, policy_id: str, subjects, actions, resources,
              effect: str = PERMIT,
              condition: Optional[Callable[[Dict[str, Any]], bool]] = None) -> None:
        self.pap.put(Policy(policy_id, tuple(subjects), tuple(actions),
                            tuple(resources), effect, condition))

    def deny(self, policy_id: str, subjects, actions, resources) -> None:
        self.grant(policy_id, subjects, actions, resources, effect=DENY)

    def revoke(self, policy_id: str) -> None:
        self.pap.remove(policy_id)

    def stats(self) -> Dict[str, Any]:
        return {"policies": len(self.pap.all_policies()),
                "version": self.pap.version, "fetches": self.pap.fetches}


class RealToDigitalSynchronization:
    """Real-to-Digital Synchronization Component.

    Paper: "implements a specific subset of Digital Models XC Functions
    related to the Simulation Domain i.e. keeping digital models in-sync with
    changes happening in the real world."

    It subscribes to the ModelUpdated events raised by Synchronization
    Services on the ledger and mirrors the corrected parameters into the Model
    Repository, so that the Simulation Domain always reads a current model.
    """

    component = "Real-to-Digital Synchronization"
    realises = XC.DIGITAL_MODELS

    def __init__(self, repository: ModelRepository):
        self.repository = repository
        self.updates_applied = 0
        self.last_event: Dict[str, Any] = {}

    def on_model_updated(self, event) -> None:
        payload = dict(event.payload)
        model_id = payload.get("model_id", "")
        current = self.repository.get(model_id) or {"payload": {}, "version": 0}
        merged = dict(current["payload"])
        merged[payload["param"]] = payload["value"]
        self.repository.put(model_id, merged,
                            version=int(payload.get("version", current["version"] + 1)))
        self.updates_applied += 1
        self.last_event = payload

    def stats(self) -> Dict[str, Any]:
        return {"updates_applied": self.updates_applied,
                "models": self.repository.ids()}


class PlatformManagement:
    """Platform Management Component.

    Paper: "implements the Cloud part of the Management XC Functions as a
    platform cloud tool."  It drives the Node Management hooks of every Edge
    Gateway: monitoring, commissioning and decommissioning.
    """

    component = "Platform Management"
    realises = XC.MANAGEMENT

    def __init__(self, wan_link=None):
        self._nodes: Dict[str, Any] = {}
        self._wan = wan_link
        self.calls = 0

    def register_node(self, node_management) -> None:
        self._nodes[node_management.gateway_id] = node_management

    def invoke(self, gateway_id: str, endpoint: str, **kwargs: Any) -> Dict[str, Any]:
        node = self._nodes.get(gateway_id)
        if node is None:
            return {"ok": False, "reason": f"unknown node {gateway_id}"}
        if self._wan is not None:
            self._wan.transmit(224, "MANAGEMENT_CALL")
        self.calls += 1
        return node.invoke(endpoint, **kwargs)

    def sweep(self, endpoint: str = "heartbeat") -> Dict[str, Dict[str, Any]]:
        """Poll every registered Edge Gateway."""
        return {gid: self.invoke(gid, endpoint) for gid in sorted(self._nodes)}

    def inventory(self) -> List[Dict[str, Any]]:
        return [node.get_status() for node in self._nodes.values()]

    def stats(self) -> Dict[str, Any]:
        return {"nodes": len(self._nodes), "management_calls": self.calls}
