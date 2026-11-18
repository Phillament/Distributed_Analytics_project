"""Cloud Tier -- the top layer of the FAR-EDGE RA.

Paper, S2.3: "The Cloud Tier is the top layer of the FAR-EDGE RA.
'Traditionally' populated by Cloud Servers (CS) running the parts of the
business logic of Functional Domains and XC Functions that benefit from having
the widest of scopes over production processes ... The Cloud Tier is in
Enterprise Ecosystem scope.  Cloud Services and Applications are visible from
all Tiers."

Paper, S3.4: "The Cloud Tier includes a total of eleven components, three of
them having Enabler status."

This module assembles exactly those eleven, in the paper's own three groups,
and exposes the inventory for the conformance experiment.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from ..common.crypto import Identity
from ..common.ledger_client import CloudLedgerClients
from ..common.network import WAN_CLOUD, Link
from ..common.pdp import PolicyDecisionPoint
from ..config import Domain, Scope, Tier, XC
from ..simulation.line_simulator import LineSimulator
from .internal_services import (DataStorage, IdentityManagement,
                                ModelRepository, PlatformManagement,
                                RealToDigitalSynchronization,
                                SecurityManagement)
from .open_api import (OpenApiForAnalytics, OpenApiForAutomation,
                       OpenApiForVirtualization)

# The eleven Cloud-Tier Components (paper S3.4), with their classification.
COMPONENTS = (
    ("Open API for Automation", "ENABLER", "PUBLIC_CLOUD_SERVICE", Domain.AUTOMATION),
    ("Open API for Analytics", "ENABLER", "PUBLIC_CLOUD_SERVICE", Domain.ANALYTICS),
    ("Open API for Virtualization", "ENABLER", "PUBLIC_CLOUD_SERVICE", Domain.SIMULATION),
    ("Identity Management", "COMPONENT", "INTERNAL_CLOUD_SERVICE", XC.SECURITY),
    ("Model Repository", "COMPONENT", "INTERNAL_CLOUD_SERVICE", XC.DIGITAL_MODELS),
    ("Data Storage", "COMPONENT", "INTERNAL_CLOUD_SERVICE", "INTERNAL"),
    ("Security Management", "COMPONENT", "PLATFORM_CLOUD_TOOL", XC.SECURITY),
    ("Policy Decision Point", "COMPONENT", "PLATFORM_CLOUD_TOOL", XC.SECURITY),
    ("Ledger Clients", "COMPONENT", "PLATFORM_CLOUD_TOOL", "ALL_DOMAINS"),
    ("Real-to-Digital Synchronization", "COMPONENT", "PLATFORM_CLOUD_TOOL",
     XC.DIGITAL_MODELS),
    ("Platform Management", "COMPONENT", "PLATFORM_CLOUD_TOOL", XC.MANAGEMENT),
)


class CloudTier:
    """A Cloud Server running the eleven Cloud-Tier Components."""

    tier = Tier.CLOUD
    scope = Scope.ENTERPRISE

    def __init__(self, ledger, wan: Link, stations: List[str],
                 key_bits: int = 512):
        self.identity = Identity("CLOUD-SRV", bits=key_bits)
        self._wan = wan

        # ---- (iii) Platform Cloud Tools --------------------------------
        self.security_management = SecurityManagement()
        self.pdp = PolicyDecisionPoint("CLOUD-SRV", "CLOUD",
                                       self.security_management.pap, None)
        self.ledger_clients = CloudLedgerClients("CLOUD-SRV", ledger,
                                                 self.identity, wan)
        self.model_repository = ModelRepository()
        self.real_to_digital = RealToDigitalSynchronization(self.model_repository)
        self.platform_management = PlatformManagement(wan)

        # ---- (ii) Internal Cloud Services ------------------------------
        self.identity_management = IdentityManagement()
        self.data_storage = DataStorage()

        # ---- Simulation Domain back end --------------------------------
        self.simulator = LineSimulator(self.ledger_clients, stations)

        # ---- (i) Public Cloud Services: the three Open API Enablers ----
        self.gateways: Dict[str, Any] = {}
        self.open_api_automation = OpenApiForAutomation(
            self.identity_management, self.pdp, self.ledger_clients, wan,
            self.gateways)
        self.open_api_analytics = OpenApiForAnalytics(
            self.identity_management, self.pdp, self.ledger_clients, wan,
            self.gateways, self.data_storage)
        self.open_api_virtualization = OpenApiForVirtualization(
            self.identity_management, self.pdp, self.ledger_clients, wan,
            self.model_repository, self.simulator)

        # Real-to-Digital Synchronization follows the ledger's model updates.
        self.ledger_clients.on("ModelUpdated", self.real_to_digital.on_model_updated)

    # ------------------------------------------------------------------
    # wiring
    # ------------------------------------------------------------------
    def attach_gateway(self, gateway) -> None:
        """Make an Edge Gateway visible to the Cloud Tier."""
        self.gateways[gateway.gateway_id] = gateway
        self.platform_management.register_node(gateway.node_management)

    def ingest_stream(self, record) -> None:
        """Sink for the cloud-bound, pre-processed data channel."""
        self.data_storage.store(record)

    # ------------------------------------------------------------------
    # convenience
    # ------------------------------------------------------------------
    def login(self, subject: str, secret: str = "") -> str:
        session = self.identity_management.single_sign_on(subject, secret)
        return session.token if session else ""

    def component_inventory(self) -> List[Dict[str, str]]:
        instances = {
            "Open API for Automation": self.open_api_automation,
            "Open API for Analytics": self.open_api_analytics,
            "Open API for Virtualization": self.open_api_virtualization,
            "Identity Management": self.identity_management,
            "Model Repository": self.model_repository,
            "Data Storage": self.data_storage,
            "Security Management": self.security_management,
            "Policy Decision Point": self.pdp,
            "Ledger Clients": self.ledger_clients,
            "Real-to-Digital Synchronization": self.real_to_digital,
            "Platform Management": self.platform_management,
        }
        return [{"component": name, "status": status, "classification": group,
                 "realises": realises, "class": type(instances[name]).__name__}
                for name, status, group, realises in COMPONENTS]

    def stats(self) -> Dict[str, Any]:
        return {
            "open_api_automation": self.open_api_automation.stats(),
            "open_api_analytics": self.open_api_analytics.stats(),
            "open_api_virtualization": self.open_api_virtualization.stats(),
            "identity_management": self.identity_management.stats(),
            "model_repository": self.model_repository.stats(),
            "data_storage": self.data_storage.stats(),
            "security_management": self.security_management.stats(),
            "pdp": self.pdp.stats.as_dict(),
            "ledger_clients": self.ledger_clients.stats(),
            "real_to_digital": self.real_to_digital.stats(),
            "platform_management": self.platform_management.stats(),
            "simulator": self.simulator.stats(),
        }
