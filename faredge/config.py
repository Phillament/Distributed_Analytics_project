"""Platform-wide configuration for the FAR-EDGE reference implementation.

Every tunable of the simulated deployment lives here so that experiments are
reproducible and so that the structural choices of the paper's Reference
Architecture (RA) -- Scopes and Tiers -- are explicit.

Paper reference: Isaja, M. & Soldatos, J. K. (2018), "Distributed Ledger
Architecture for Automation, Analytics and Simulation in Industrial
Environments", IFAC-PapersOnLine 51-11, 370-375.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Dict


# --------------------------------------------------------------------------
# Structural concepts of the RA (Section 2.3 of the paper)
# --------------------------------------------------------------------------
class Scope:
    """Coarse mapping of system elements (paper S2.3)."""

    PLANT = "PLANT"
    ENTERPRISE = "ENTERPRISE_ECOSYSTEM"


class Tier:
    """The four Tiers of the FAR-EDGE RA (paper S2.3, Fig. 1)."""

    FIELD = "FIELD"
    EDGE = "EDGE"
    LEDGER = "LEDGER"
    CLOUD = "CLOUD"


class Domain:
    """The three high-level Functional Domains (paper S2.2)."""

    AUTOMATION = "AUTOMATION"
    ANALYTICS = "ANALYTICS"
    SIMULATION = "SIMULATION"


class XC:
    """The four Crosscutting Functions (paper S2.2)."""

    MANAGEMENT = "MANAGEMENT"
    SECURITY = "SECURITY"
    DIGITAL_MODELS = "DIGITAL_MODELS"
    FIELD_ABSTRACTION_DATA_ROUTING = "FIELD_ABSTRACTION_AND_DATA_ROUTING"


# --------------------------------------------------------------------------
# Network model
# --------------------------------------------------------------------------
@dataclass
class NetworkConfig:
    """Latency model of the links between Tiers.

    The paper motivates the Edge Tier with *proximity* ("deploying computing
    power and data storage in close proximity to where it is actually used ...
    helps reducing network latency").  To be able to quantify that claim we
    model each class of link with a base latency plus jitter.  Values are
    representative orders of magnitude for an industrial deployment.
    """

    # Field device  <-> Edge Gateway  (shop-floor LAN / fieldbus)
    field_to_edge_ms: float = 1.0
    field_to_edge_jitter_ms: float = 0.4

    # Edge Gateway  <-> Edge Gateway  (plant LAN)
    plant_lan_ms: float = 2.0
    plant_lan_jitter_ms: float = 0.8

    # Peer Node     <-> Peer Node     (distributed ledger p2p overlay)
    peer_to_peer_ms: float = 4.0
    peer_to_peer_jitter_ms: float = 1.5

    # Plant         <-> Cloud Tier    (WAN / Internet, HTTPS)
    edge_to_cloud_ms: float = 45.0
    edge_to_cloud_jitter_ms: float = 12.0

    # Probability that a message on the WAN needs a retransmission
    wan_loss_prob: float = 0.0

    # If False, link delays are accounted for analytically but not slept on.
    # Real sleeping gives a genuinely concurrent run; accounting-only gives
    # fast, deterministic large-scale runs.
    real_sleep: bool = True

    # Global speed factor applied to real sleeps (1.0 = model values).
    time_scale: float = 1.0


# --------------------------------------------------------------------------
# Ledger Tier
# --------------------------------------------------------------------------
@dataclass
class LedgerConfig:
    """Parameters of the Distributed Ledger Enabler (paper S3.3).

    The paper states two distinct consensus thresholds, which we implement
    verbatim:
      * "A majority consensus of PNs is also required for the creation of
        every new record, which constitutes an atomic transaction."
      * records "cannot be modified or removed once created, unless there is
        a system-wide unanimous consensus on doing that."
    """

    n_peer_nodes: int = 5
    # The paper says "a majority consensus of PNs" without saying majority of
    # *which* set.  Both readings are implemented and selectable:
    #   "known"  -- majority of all Peer Nodes in the system (the standard
    #               quorum rule; tolerates f < n/2 crash faults and cannot
    #               split-brain).  Default.
    #   "online" -- majority of the Peer Nodes that "happen to be online at
    #               the moment" (S2.3), which keeps writes available with very
    #               few peers but admits divergence under a partition.
    # Experiment 5 measures the difference.
    quorum_basis: str = "known"
    # Transactions are batched into blocks; a block is proposed when either
    # the batch is full or the batch timeout expires.
    block_max_tx: int = 16
    block_timeout_s: float = 0.25
    # How long a non-empty batch lingers waiting for companions before it is
    # proposed.  Keeps commit latency dominated by the consensus rounds rather
    # than by batching, which is the realistic regime for a shop floor whose
    # write rate is low by design.
    block_linger_s: float = 0.005
    rsa_key_bits: int = 512             # simulation-grade keys (see crypto.py)
    # Peers that deliberately misbehave (Byzantine) - used by experiment 5.
    byzantine_peer_ids: tuple = ()
    # Peers that are offline (crash faults) - used by experiment 5.
    crashed_peer_ids: tuple = ()


# --------------------------------------------------------------------------
# Edge Tier
# --------------------------------------------------------------------------
@dataclass
class EdgeConfig:
    """Parameters of the Edge Tier components (paper S3.2)."""

    # Data Routing & Pre-processing: dead-band filter width, as a fraction of
    # each signal's nominal value.  Samples whose change is smaller than the
    # dead band carry no information for automation and are dropped.
    deadband_fraction: float = 0.02
    # Window (number of samples) over which the Edge Analytics Engine
    # aggregates before considering a northbound publication.
    analytics_window: int = 50
    # A local KPI is republished to the Ledger Tier only when it moves by more
    # than this relative amount ("each time a local KPI value changes").
    kpi_publish_epsilon: float = 0.005
    # Local automation control loop period.
    control_period_s: float = 0.05


# --------------------------------------------------------------------------
# Field Tier
# --------------------------------------------------------------------------
@dataclass
class FieldConfig:
    """Parameters of the Field Tier (paper S3.1)."""

    sample_rate_hz: float = 10.0
    protocol: str = "MQTT"              # "MQTT" or "OPCUA" (both supported)
    enable_pdp: bool = True


# --------------------------------------------------------------------------
# Top-level configuration
# --------------------------------------------------------------------------
@dataclass
class PlatformConfig:
    n_stations: int = 4                 # physical work stations on the line
    stations_per_gateway: int = 1       # Edge Gateways = n_stations / this
    network: NetworkConfig = field(default_factory=NetworkConfig)
    ledger: LedgerConfig = field(default_factory=LedgerConfig)
    edge: EdgeConfig = field(default_factory=EdgeConfig)
    field_tier: FieldConfig = field(default_factory=FieldConfig)
    seed: int = 20180321                # DOI of the paper used as the seed
    verbose: bool = False

    @property
    def n_gateways(self) -> int:
        return max(1, self.n_stations // max(1, self.stations_per_gateway))

    def to_dict(self) -> Dict:
        return asdict(self)


DEFAULT = PlatformConfig()
