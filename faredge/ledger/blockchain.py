"""Block, hash chain and world state of the Distributed Ledger Enabler.

Paper, S3.3: "It provides a transactional ledger that is replicated on all
PNs and automatically kept in-sync across all instances, using peer-to-peer
communication only.  The ledger stores and maintains the shared state of
distributed processes: global variables and even complex data structures that
are read and written concurrently by business logic running on any Tier ...
Such state is secured by cryptographic techniques, so that records in the
ledger cannot be modified or removed once created".

The two mechanisms that deliver that guarantee are implemented here:
  * a hash chain (every block commits to its predecessor), and
  * a Merkle root over the service calls inside the block,
so that altering any committed record changes the block hash and breaks the
chain from that point on -- which `verify_integrity` detects.
"""
from __future__ import annotations

import copy
import threading
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional, Tuple

from ..common.crypto import canonical_json, hash_object, merkle_root, sha256_hex

GENESIS_PREV = "0" * 64


@dataclass
class Block:
    """An atomic batch of service calls agreed upon by a majority of Peers."""

    index: int
    prev_hash: str
    calls: List[Dict[str, Any]] = field(default_factory=list)
    timestamp: float = 0.0
    proposer: str = ""                 # address of the proposing Peer Node
    merkle: str = ""
    state_root: str = ""               # world-state hash *after* executing calls
    op: str = "APPEND"                 # APPEND | AMEND | REMOVE | DEPLOY_CONTRACT
    votes_for: int = 0
    votes_against: int = 0
    signature: str = ""
    hash: str = ""

    def compute_merkle(self) -> str:
        return merkle_root([hash_object(c) for c in self.calls])

    def header(self) -> Dict[str, Any]:
        """The signed and hashed part of the block."""
        return {
            "index": self.index,
            "prev_hash": self.prev_hash,
            "merkle": self.merkle,
            "state_root": self.state_root,
            "timestamp": self.timestamp,
            "proposer": self.proposer,
            "op": self.op,
        }

    def compute_hash(self) -> str:
        return hash_object(self.header())

    def seal(self) -> "Block":
        self.merkle = self.compute_merkle()
        self.hash = self.compute_hash()
        return self

    def size_bytes(self) -> int:
        return len(canonical_json(asdict(self)).encode("utf-8"))


class WorldState:
    """The shared state of distributed processes, keyed by Ledger Service.

    Layout: {service_name: {key: value}}.  A deterministic hash of the whole
    structure (the state root) is what Peer Nodes compare in order to reach
    consensus on the *result* of executing a block, not merely on its content.
    """

    def __init__(self, data: Optional[Dict[str, Dict[str, Any]]] = None):
        self._data: Dict[str, Dict[str, Any]] = data or {}

    def service(self, name: str) -> Dict[str, Any]:
        return self._data.setdefault(name, {})

    def snapshot(self) -> Dict[str, Dict[str, Any]]:
        return copy.deepcopy(self._data)

    def replace(self, data: Dict[str, Dict[str, Any]]) -> None:
        self._data = data

    def root(self) -> str:
        return hash_object(self._data)

    def read(self, service: str, key: str, default: Any = None) -> Any:
        return self._data.get(service, {}).get(key, default)

    def as_dict(self) -> Dict[str, Dict[str, Any]]:
        return self._data


class Blockchain:
    """One replica of the ledger, as held by a single Peer Node."""

    def __init__(self, genesis_state_root: str = ""):
        self._lock = threading.RLock()
        genesis = Block(index=0, prev_hash=GENESIS_PREV, calls=[],
                        timestamp=0.0, proposer="genesis",
                        state_root=genesis_state_root, op="GENESIS")
        genesis.seal()
        self.blocks: List[Block] = [genesis]

    # -- accessors -------------------------------------------------------
    @property
    def head(self) -> Block:
        with self._lock:
            return self.blocks[-1]

    @property
    def height(self) -> int:
        with self._lock:
            return len(self.blocks) - 1

    def __len__(self) -> int:
        return len(self.blocks)

    def all_calls(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [c for b in self.blocks for c in b.calls]

    def size_bytes(self) -> int:
        return sum(b.size_bytes() for b in self.blocks)

    # -- mutation --------------------------------------------------------
    def append(self, block: Block) -> None:
        with self._lock:
            if block.prev_hash != self.blocks[-1].hash:
                raise ValueError(
                    f"block {block.index} does not extend head "
                    f"{self.blocks[-1].index}")
            self.blocks.append(block)

    # -- integrity -------------------------------------------------------
    def verify_integrity(self) -> Tuple[bool, str]:
        """Re-derive every hash and check the chain.

        This is the function that makes the paper's immutability claim
        testable: experiment 5 mutates a committed record and shows that this
        check fails, i.e. tampering cannot go unnoticed.
        """
        with self._lock:
            for i, b in enumerate(self.blocks):
                if b.index != i:
                    return False, f"block at position {i} claims index {b.index}"
                if b.merkle != b.compute_merkle():
                    return False, f"merkle root mismatch in block {i}"
                if b.hash != b.compute_hash():
                    return False, f"header hash mismatch in block {i}"
                if i == 0:
                    if b.prev_hash != GENESIS_PREV:
                        return False, "genesis prev_hash is not zero"
                elif b.prev_hash != self.blocks[i - 1].hash:
                    return False, f"broken link between blocks {i-1} and {i}"
        return True, "chain intact"

    def fingerprint(self) -> str:
        """Short digest of the whole chain, used to compare replicas."""
        return sha256_hex("".join(b.hash for b in self.blocks))[:16]
