"""Consensus protocol of the Distributed Ledger Enabler.

The paper specifies the consensus requirements precisely, and this module
implements them literally (S3.3):

  (a) "each service call is executed in parallel by all Peer Nodes that happen
      to be online at the moment, which then need to reach a consensus on its
      validity" (S2.3)
  (b) "A majority consensus of PNs is also required for the creation of every
      new record, which constitutes an atomic transaction."
  (c) records "cannot be modified or removed once created, unless there is a
      system-wide unanimous consensus on doing that."

The protocol is a three-phase, PBFT-flavoured agreement:

    PRE-PREPARE   the round's proposer executes the batch, builds and signs a
                  candidate block, and broadcasts it over the peer-to-peer
                  overlay.
    PREPARE       every online peer *independently re-executes* the batch
                  against its own replica and votes on whether the resulting
                  state root matches the proposer's.  This is what makes a
                  Byzantine proposer detectable: it cannot forge a state root
                  that honest peers will reproduce.
    COMMIT        if the quorum for the requested operation is met, every
                  honest online peer appends the block; otherwise the block is
                  discarded and the next peer takes a turn as proposer.

Quorum rules, from (b) and (c):
    APPEND / DEPLOY_CONTRACT -> strict majority of *online* peers
    AMEND / REMOVE           -> unanimity of *all known* peers
"""
from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Sequence

from ..common.crypto import KeyDirectory
from ..common.messages import TX_AMEND, TX_APPEND, TX_DEPLOY, TX_REMOVE
from ..common.network import PEER_P2P, Link
from .blockchain import Block
from .peer_node import PeerNode, Vote

MAJORITY_OPS = (TX_APPEND, TX_DEPLOY)
UNANIMOUS_OPS = (TX_AMEND, TX_REMOVE)


@dataclass
class ConsensusOutcome:
    committed: bool
    block: Block | None
    op: str
    votes_for: int = 0
    votes_against: int = 0
    peers_online: int = 0
    peers_known: int = 0
    quorum_required: int = 0
    rounds: int = 1
    reason: str = ""
    outcomes: List[Dict[str, Any]] = field(default_factory=list)
    events: List[Dict[str, Any]] = field(default_factory=list)
    latency_ms: float = 0.0
    p2p_messages: int = 0


def quorum_required(op: str, n_online: int, n_known: int,
                    basis: str = "known") -> int:
    """Number of approving votes needed for an operation.

    AMEND and REMOVE always need "a system-wide unanimous consensus"; APPEND
    and DEPLOY_CONTRACT need "a majority consensus of PNs", where `basis`
    selects which set of Peer Nodes the majority is taken over (see
    LedgerConfig.quorum_basis).
    """
    if op in UNANIMOUS_OPS:
        return n_known                      # system-wide unanimous consensus
    population = n_online if basis == "online" else n_known
    return math.floor(population / 2) + 1   # strict majority


class ConsensusEngine:
    """Drives one round of agreement across a set of Peer Nodes."""

    def __init__(self, peers: Sequence[PeerNode], directory: KeyDirectory,
                 p2p: Link, max_rounds: int = 3, quorum_basis: str = "known"):
        self.peers = list(peers)
        self.directory = directory
        self.p2p = p2p
        self.max_rounds = max_rounds
        self.quorum_basis = quorum_basis
        self._round = 0

    # -- helpers ---------------------------------------------------------
    @property
    def online_peers(self) -> List[PeerNode]:
        return [p for p in self.peers if p.online]

    def _next_proposer(self) -> PeerNode | None:
        online = self.online_peers
        if not online:
            return None
        proposer = online[self._round % len(online)]
        self._round += 1
        return proposer

    # -- the protocol ----------------------------------------------------
    def agree(self, calls: List[Dict[str, Any]], op: str = TX_APPEND
              ) -> ConsensusOutcome:
        started = time.perf_counter()
        p2p_messages = 0
        last_reason = "no online peer could propose"

        for attempt in range(1, self.max_rounds + 1):
            online = self.online_peers
            if not online:
                return ConsensusOutcome(False, None, op, peers_known=len(self.peers),
                                        reason="no Peer Node online",
                                        rounds=attempt)
            proposer = self._next_proposer()
            if proposer is None:                       # pragma: no cover
                break

            # ---- PRE-PREPARE ------------------------------------------
            block, outcomes, events = proposer.propose(calls, op, time.time())
            validators = [p for p in online if p is not proposer]
            for _ in validators:
                self.p2p.transmit(block.size_bytes(), "BLOCK_PROPOSAL")
                p2p_messages += 1

            # ---- PREPARE (independent re-execution) -------------------
            votes: List[Vote] = [Vote(peer=proposer.address,
                                      accept=not proposer.byzantine,
                                      state_root=block.state_root,
                                      reason="proposer self-vote")]
            for peer in validators:
                vote = peer.validate(block, self.directory)
                votes.append(vote)
                self.p2p.transmit(256, "VOTE")
                p2p_messages += 1

            votes_for = sum(1 for v in votes if v.accept)
            votes_against = len(votes) - votes_for
            needed = quorum_required(op, len(online), len(self.peers),
                                     self.quorum_basis)

            # ---- COMMIT ------------------------------------------------
            if votes_for >= needed:
                block.votes_for = votes_for
                block.votes_against = votes_against
                committed_events: List[Dict[str, Any]] = []
                for peer in online:
                    if peer.byzantine:
                        # A Byzantine peer may refuse to commit; the honest
                        # majority still carries the chain forward.
                        continue
                    evs = peer.commit(block)
                    if peer is proposer or not committed_events:
                        committed_events = evs
                    self.p2p.transmit(128, "COMMIT_ACK")
                    p2p_messages += 1
                return ConsensusOutcome(
                    True, block, op, votes_for, votes_against, len(online),
                    len(self.peers), needed, attempt, "committed",
                    outcomes, committed_events or events,
                    (time.perf_counter() - started) * 1000.0, p2p_messages)

            last_reason = (f"quorum not met: {votes_for}/{needed} approvals "
                           f"({votes_against} against)")
            rejections = [v.reason for v in votes if not v.accept and v.reason]
            if rejections:
                last_reason += f"; first objection: {rejections[0]}"

        return ConsensusOutcome(False, None, op, 0, 0, len(self.online_peers),
                                len(self.peers),
                                quorum_required(op, len(self.online_peers),
                                                len(self.peers),
                                                self.quorum_basis),
                                self.max_rounds, last_reason, [], [],
                                (time.perf_counter() - started) * 1000.0,
                                p2p_messages)
