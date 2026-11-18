"""Network model and instrumentation.

Three of the paper's four motivations for edge computing are network
properties: saving bandwidth, enabling low-latency proximity processing and
keeping the shop floor isolated (S1.2).  None of them can be assessed without
measuring traffic, so every byte that crosses a tier boundary in this
implementation passes through a Link and is accounted for by the
NetworkMonitor.
"""
from __future__ import annotations

import random
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List

from .config_bridge import get_network_config


# Link classes, named after the boundaries they cross.
FIELD_LAN = "field_lan"       # Field Tier  <-> Edge Tier   (inside the plant)
PLANT_LAN = "plant_lan"       # Edge Tier   <-> Edge Tier   (inside the plant)
PEER_P2P = "peer_p2p"         # Peer Node   <-> Peer Node   (ledger overlay)
WAN_CLOUD = "wan_cloud"       # Plant Scope <-> Enterprise Ecosystem Scope

PLANT_INTERNAL = (FIELD_LAN, PLANT_LAN)


@dataclass
class LinkStats:
    messages: int = 0
    bytes: int = 0
    latency_ms: List[float] = field(default_factory=list)
    retransmissions: int = 0

    def add(self, nbytes: int, latency_ms: float) -> None:
        self.messages += 1
        self.bytes += nbytes
        self.latency_ms.append(latency_ms)

    @property
    def mean_latency_ms(self) -> float:
        return sum(self.latency_ms) / len(self.latency_ms) if self.latency_ms else 0.0


class NetworkMonitor:
    """Thread-safe traffic counters, grouped by link class."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.stats: Dict[str, LinkStats] = defaultdict(LinkStats)
        # Every payload kind that ever left Plant Scope, for the isolation
        # check of experiment 2.
        self.egress_payload_kinds: Dict[str, int] = defaultdict(int)

    def record(self, link_class: str, nbytes: int, latency_ms: float,
               payload_kind: str = "") -> None:
        with self._lock:
            self.stats[link_class].add(nbytes, latency_ms)
            if link_class == WAN_CLOUD and payload_kind:
                self.egress_payload_kinds[payload_kind] += 1

    def record_retransmission(self, link_class: str) -> None:
        with self._lock:
            self.stats[link_class].retransmissions += 1

    # -- queries ---------------------------------------------------------
    def bytes_on(self, link_class: str) -> int:
        return self.stats[link_class].bytes

    def messages_on(self, link_class: str) -> int:
        return self.stats[link_class].messages

    def total_bytes(self) -> int:
        return sum(s.bytes for s in self.stats.values())

    def plant_internal_bytes(self) -> int:
        return sum(self.stats[c].bytes for c in PLANT_INTERNAL)

    def egress_bytes(self) -> int:
        """Bytes that crossed the Plant / Enterprise Ecosystem boundary."""
        return self.stats[WAN_CLOUD].bytes

    def summary(self) -> Dict[str, Dict[str, float]]:
        out: Dict[str, Dict[str, float]] = {}
        for name, s in sorted(self.stats.items()):
            lat = sorted(s.latency_ms)
            out[name] = {
                "messages": s.messages,
                "bytes": s.bytes,
                "mean_latency_ms": round(s.mean_latency_ms, 3),
                "p95_latency_ms": round(lat[int(0.95 * (len(lat) - 1))], 3) if lat else 0.0,
                "retransmissions": s.retransmissions,
            }
        return out

    def reset(self) -> None:
        with self._lock:
            self.stats.clear()
            self.egress_payload_kinds.clear()


MONITOR = NetworkMonitor()

_SEED = 0xFA2ED6E


class Link:
    """A communication link with a latency distribution.

    transmit() returns the modelled one-way latency in milliseconds and, when
    real_sleep is enabled, actually blocks for it so that the simulation is
    genuinely concurrent (Peer Nodes and Edge Gateways run in real threads).
    """

    def __init__(self, link_class: str, base_ms: float, jitter_ms: float,
                 loss_prob: float = 0.0, rng: random.Random | None = None):
        self.link_class = link_class
        self.base_ms = base_ms
        self.jitter_ms = jitter_ms
        self.loss_prob = loss_prob
        self._rng = rng or random.Random(_SEED)
        self._rng_lock = threading.Lock()

    def _draw(self) -> float:
        if not self.jitter_ms:
            return self.base_ms
        with self._rng_lock:
            jitter = abs(self._rng.gauss(0.0, self.jitter_ms / 2.0))
        return self.base_ms + jitter

    def transmit(self, payload_bytes: int, payload_kind: str = "") -> float:
        cfg = get_network_config()
        latency = self._draw()
        attempts = 1
        while self.loss_prob:
            with self._rng_lock:
                lost = self._rng.random() < self.loss_prob
            if not lost:
                break
            MONITOR.record_retransmission(self.link_class)
            latency += self._draw() * 2.0      # timeout + retry
            attempts += 1
            if attempts > 5:
                break
        if cfg.real_sleep and latency > 0:
            time.sleep(latency / 1000.0 * cfg.time_scale)
        MONITOR.record(self.link_class, payload_bytes * attempts, latency, payload_kind)
        return latency


def build_links(cfg) -> Dict[str, Link]:
    """Instantiate one Link per link class from a NetworkConfig."""
    rng = random.Random(_SEED)
    return {
        FIELD_LAN: Link(FIELD_LAN, cfg.field_to_edge_ms,
                        cfg.field_to_edge_jitter_ms, 0.0, rng),
        PLANT_LAN: Link(PLANT_LAN, cfg.plant_lan_ms,
                        cfg.plant_lan_jitter_ms, 0.0, rng),
        PEER_P2P: Link(PEER_P2P, cfg.peer_to_peer_ms,
                       cfg.peer_to_peer_jitter_ms, 0.0, rng),
        WAN_CLOUD: Link(WAN_CLOUD, cfg.edge_to_cloud_ms,
                        cfg.edge_to_cloud_jitter_ms, cfg.wan_loss_prob, rng),
    }
