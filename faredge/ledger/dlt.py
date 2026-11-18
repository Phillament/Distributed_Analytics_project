"""Distributed Ledger Enabler -- the facade over the Peer Node set.

Paper, S3.3: "The Distributed Ledger Enabler is the foundation of the Ledger
Tier.  It provides a transactional ledger that is replicated on all PNs and
automatically kept in-sync across all instances, using peer-to-peer
communication only."

This class is what the rest of the Platform talks to.  It:
  * owns the Peer Nodes and the key directory,
  * installs the four Ledger Services Components on every peer,
  * batches incoming service calls into blocks and runs consensus on them in
    a background thread (so writes are asynchronous and concurrent, as they
    would be in a real deployment),
  * serves reads from any online replica *without* consensus -- "The ideal
    clients perform frequent reads and infrequent writes of the shared state,
    the latter based on fast and simple logic" (S3.3),
  * dispatches ledger events to subscribers (Edge-Tier Ledger Clients),
  * and offers fault injection so that the reliability claims of the paper can
    be tested.
"""
from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from ..common.crypto import Identity, KeyDirectory
from ..common.messages import (LedgerEvent, Receipt, ServiceCall, TX_APPEND,
                               TX_DEPLOY)
from ..common.network import PEER_P2P, Link
from ..config import LedgerConfig, Tier
from .consensus import ConsensusEngine, ConsensusOutcome, quorum_required
from .peer_node import PeerNode
from .services.configuration import ConfigurationServices
from .services.data_publishing import DataPublishingServices
from .services.orchestration import OrchestrationServices
from .services.synchronization import SynchronizationServices

# The four Ledger Services Components of the paper (Fig. 2, S3.3).
LEDGER_SERVICE_FACTORIES: Tuple[Callable[[], Any], ...] = (
    OrchestrationServices,
    ConfigurationServices,
    DataPublishingServices,
    SynchronizationServices,
)


@dataclass
class _Pending:
    call: ServiceCall
    signature: str
    done: threading.Event = field(default_factory=threading.Event)
    receipt: Optional[Receipt] = None
    submitted_at: float = field(default_factory=time.perf_counter)


@dataclass
class LedgerStats:
    blocks_committed: int = 0
    tx_submitted: int = 0
    tx_committed: int = 0
    tx_rejected_by_contract: int = 0
    tx_failed_consensus: int = 0
    consensus_rounds: int = 0
    commit_latencies_ms: List[float] = field(default_factory=list)
    consensus_latencies_ms: List[float] = field(default_factory=list)
    events_emitted: int = 0

    def as_dict(self) -> Dict[str, Any]:
        lat = sorted(self.commit_latencies_ms)
        return {
            "blocks_committed": self.blocks_committed,
            "tx_submitted": self.tx_submitted,
            "tx_committed": self.tx_committed,
            "tx_rejected_by_contract": self.tx_rejected_by_contract,
            "tx_failed_consensus": self.tx_failed_consensus,
            "events_emitted": self.events_emitted,
            "mean_commit_latency_ms": round(sum(lat) / len(lat), 3) if lat else 0.0,
            "p95_commit_latency_ms": round(lat[int(0.95 * (len(lat) - 1))], 3) if lat else 0.0,
            "mean_tx_per_block": round(self.tx_committed / self.blocks_committed, 2)
            if self.blocks_committed else 0.0,
        }


class DistributedLedger:
    """The Ledger Tier as seen by its clients."""

    def __init__(self, cfg: LedgerConfig, p2p_link: Link,
                 host_tiers: Optional[Sequence[str]] = None):
        self.cfg = cfg
        self.directory = KeyDirectory()
        self.p2p = p2p_link
        self.stats = LedgerStats()

        # Peer Nodes may live in any Tier - the Ledger Tier is logical (S3.3).
        tiers = list(host_tiers or [])
        self.peers: List[PeerNode] = []
        for i in range(cfg.n_peer_nodes):
            node_id = f"PN-{i}"
            peer = PeerNode(node_id, key_bits=cfg.rsa_key_bits,
                            host_tier=tiers[i] if i < len(tiers) else Tier.EDGE,
                            byzantine=(i in cfg.byzantine_peer_ids))
            peer.online = i not in cfg.crashed_peer_ids
            # Every Peer Node gets its own instances of every Ledger Service.
            for factory in LEDGER_SERVICE_FACTORIES:
                peer.install(factory())
            self.directory.register(peer.identity)
            self.peers.append(peer)

        self.engine = ConsensusEngine(self.peers, self.directory, p2p_link,
                                      quorum_basis=cfg.quorum_basis)
        self._queue: "queue.Queue[_Pending]" = queue.Queue()
        self._subscribers: List[Tuple[str, Callable[[LedgerEvent], None]]] = []
        self._sub_lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._read_cursor = 0
        self.event_log: List[LedgerEvent] = []

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------
    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._committer, daemon=True,
                                        name="ledger-committer")
        self._thread.start()

    def stop(self, drain_timeout: float = 5.0) -> None:
        deadline = time.time() + drain_timeout
        while not self._queue.empty() and time.time() < deadline:
            time.sleep(0.005)
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None

    # ------------------------------------------------------------------
    # client registration
    # ------------------------------------------------------------------
    def register_client(self, identity: Identity) -> str:
        """Make a Ledger Client's public key known to every Peer Node."""
        return self.directory.register(identity)

    # ------------------------------------------------------------------
    # write path
    # ------------------------------------------------------------------
    def submit(self, call: ServiceCall, identity: Identity,
               timeout: float = 10.0) -> Receipt:
        """Submit a signed service call and block until it is decided."""
        signature = identity.sign(call.digest())
        pending = _Pending(call=call, signature=signature)
        self.stats.tx_submitted += 1
        self._queue.put(pending)
        if not self._running:
            # Synchronous fallback: useful in unit tests.
            self._drain_once()
        if not pending.done.wait(timeout):
            return Receipt(call_id=call.id, accepted=False,
                           reason="timed out waiting for consensus")
        assert pending.receipt is not None
        return pending.receipt

    def deploy_contract(self, name: str, source: str,
                        identity: Identity) -> Receipt:
        """Deploy or update a Ledger Service through a ledger transaction."""
        call = ServiceCall(service="__contracts__", endpoint="deploy",
                           args={"name": name, "source": source},
                           caller=identity.address, op=TX_DEPLOY)
        return self.submit(call, identity)

    # ------------------------------------------------------------------
    # read path (no consensus)
    # ------------------------------------------------------------------
    def _pick_replica(self) -> PeerNode:
        online = [p for p in self.peers if p.online and not p.byzantine]
        if not online:
            online = [p for p in self.peers if p.online] or self.peers
        self._read_cursor = (self._read_cursor + 1) % len(online)
        return online[self._read_cursor]

    def read(self, service: str, key: str, default: Any = None) -> Any:
        return self._pick_replica().read(service, key, default)

    def service_state(self, service: str) -> Dict[str, Any]:
        return self._pick_replica().service_state(service)

    # ------------------------------------------------------------------
    # events
    # ------------------------------------------------------------------
    def subscribe(self, service: str,
                  callback: Callable[[LedgerEvent], None]) -> None:
        """Subscribe to ledger events of one service ("" = all services)."""
        with self._sub_lock:
            self._subscribers.append((service, callback))

    def _dispatch(self, events: List[Dict[str, Any]], block_index: int) -> None:
        with self._sub_lock:
            subs = list(self._subscribers)
        for raw in events:
            event = LedgerEvent(service=raw["service"], name=raw["name"],
                                payload=raw["payload"], block=block_index)
            self.event_log.append(event)
            self.stats.events_emitted += 1
            for service, callback in subs:
                if service in ("", event.service):
                    try:
                        callback(event)
                    except Exception:          # a bad subscriber must not
                        pass                   # break the ledger
    # ------------------------------------------------------------------
    # committer thread
    # ------------------------------------------------------------------
    def _committer(self) -> None:
        while self._running:
            self._drain_once(block=True)

    def _drain_once(self, block: bool = False) -> None:
        batch: List[_Pending] = []
        try:
            first = self._queue.get(timeout=0.02) if block else self._queue.get_nowait()
        except queue.Empty:
            return
        batch.append(first)

        deadline = time.perf_counter() + self.cfg.block_linger_s
        while len(batch) < self.cfg.block_max_tx:
            remaining = deadline - time.perf_counter()
            if remaining <= 0:
                break
            try:
                nxt = self._queue.get(timeout=remaining)
            except queue.Empty:
                break
            batch.append(nxt)

        # A block carries a single operation class, because the quorum rule
        # depends on it (majority for APPEND, unanimity for AMEND/REMOVE).
        group: List[_Pending] = []
        for item in batch:
            if group and item.call.op != group[0].call.op:
                self._run_consensus(group)
                group = []
            group.append(item)
        if group:
            self._run_consensus(group)

    def _run_consensus(self, group: List[_Pending]) -> None:
        op = group[0].call.op
        calls = [{"call": p.call.digest(), "signature": p.signature}
                 for p in group]
        outcome: ConsensusOutcome = self.engine.agree(calls, op=op)
        self.stats.consensus_rounds += outcome.rounds
        self.stats.consensus_latencies_ms.append(outcome.latency_ms)

        if outcome.committed and outcome.block is not None:
            self.stats.blocks_committed += 1
            self._dispatch(outcome.events, outcome.block.index)
            for pending, call_outcome in zip(group, outcome.outcomes):
                accepted = bool(call_outcome["ok"])
                if accepted:
                    self.stats.tx_committed += 1
                else:
                    self.stats.tx_rejected_by_contract += 1
                latency = (time.perf_counter() - pending.submitted_at) * 1000.0
                self.stats.commit_latencies_ms.append(latency)
                pending.receipt = Receipt(
                    call_id=pending.call.id, accepted=accepted,
                    block_index=outcome.block.index,
                    votes_for=outcome.votes_for,
                    votes_against=outcome.votes_against,
                    peers_online=outcome.peers_online,
                    reason=call_outcome.get("reason", ""),
                    result=call_outcome.get("result", {}),
                    latency_ms=latency)
                pending.done.set()
        else:
            for pending in group:
                self.stats.tx_failed_consensus += 1
                pending.receipt = Receipt(
                    call_id=pending.call.id, accepted=False,
                    votes_for=outcome.votes_for,
                    votes_against=outcome.votes_against,
                    peers_online=outcome.peers_online,
                    reason=outcome.reason,
                    latency_ms=(time.perf_counter() - pending.submitted_at) * 1000.0)
                pending.done.set()

    # ------------------------------------------------------------------
    # fault injection and health
    # ------------------------------------------------------------------
    def crash(self, index: int) -> None:
        """Take a Peer Node offline (crash fault)."""
        self.peers[index].online = False

    def recover(self, index: int) -> int:
        """Bring a Peer Node back and re-synchronise it from a live replica."""
        peer = self.peers[index]
        peer.online = True
        donor = max((p for p in self.peers
                     if p is not peer and p.online and not p.byzantine),
                    key=lambda p: p.chain.height, default=None)
        if donor is None:
            return 0
        applied = peer.sync_from(donor)
        self.p2p.transmit(donor.chain.size_bytes(), "CHAIN_SYNC")
        return applied

    def make_byzantine(self, index: int, value: bool = True) -> None:
        self.peers[index].byzantine = value

    @property
    def n_online(self) -> int:
        return sum(1 for p in self.peers if p.online)

    def quorum_for(self, op: str = TX_APPEND) -> int:
        return quorum_required(op, self.n_online, len(self.peers),
                               self.cfg.quorum_basis)

    def verify_replicas(self) -> Dict[str, Any]:
        """Check that all honest online replicas agree, and that each chain is
        internally intact."""
        honest = [p for p in self.peers if p.online and not p.byzantine]
        fingerprints = {p.node_id: p.chain.fingerprint() for p in honest}
        state_roots = {p.node_id: p.state.root() for p in honest}
        integrity = {p.node_id: p.chain.verify_integrity() for p in self.peers}
        return {
            "replicas_checked": len(honest),
            "chains_identical": len(set(fingerprints.values())) <= 1,
            "states_identical": len(set(state_roots.values())) <= 1,
            "all_chains_intact": all(ok for ok, _ in integrity.values()),
            "heights": {p.node_id: p.chain.height for p in self.peers},
            "integrity": {k: v[1] for k, v in integrity.items()},
            "fingerprints": fingerprints,
        }

    def chain_height(self) -> int:
        return max(p.chain.height for p in self.peers)

    def ledger_size_bytes(self) -> int:
        return self._pick_replica().chain.size_bytes()

    def installed_services(self) -> List[str]:
        return self.peers[0].registry.names()
