"""Peer Node -- the physical host of the Distributed Ledger Enabler.

Paper, S3.3: "We stress the term 'logical' as the FAR-EDGE Platform
architecture does not mandate any specific deployment choice for Peer Nodes
(PN) -- i.e., the physical server machines that implement both the distributed
ledger and the smart contracts.  These can be installed in environments
belonging to the Cloud or Edge Tiers, possibly even to the Field in some
cases."

Accordingly a PeerNode carries a `host_tier` attribute: the Ledger Tier is
logical and its peers may be hosted anywhere.

Each Peer Node owns:
  * a full replica of the chain (`blockchain.Blockchain`),
  * a full replica of the world state (`blockchain.WorldState`),
  * its own instance of every Ledger Service (`ContractRegistry`),
so that, per S2.3, "each service call is executed in parallel by all Peer
Nodes that happen to be online at the moment, which then need to reach a
consensus on its validity."
"""
from __future__ import annotations

import copy
import threading
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from ..common.crypto import Identity, KeyDirectory, hash_object
from ..config import Tier
from .blockchain import Block, Blockchain, WorldState
from .smart_contract import (ContractRegistry, ContractResult, SmartContract,
                             compile_contract)


@dataclass
class Vote:
    peer: str
    accept: bool
    state_root: str = ""
    reason: str = ""
    signature: str = ""


class PeerNode:
    """One replica of the distributed ledger plus its smart-contract engine."""

    def __init__(self, node_id: str, key_bits: int = 512,
                 host_tier: str = Tier.EDGE, byzantine: bool = False):
        self.node_id = node_id
        self.identity = Identity(node_id, bits=key_bits)
        self.host_tier = host_tier          # Ledger Tier is logical (S3.3)
        self.byzantine = byzantine
        self.online = True

        self.state = WorldState()
        self.chain = Blockchain(genesis_state_root=self.state.root())
        self.registry = ContractRegistry()
        self._lock = threading.RLock()
        self.validations = 0
        self.rejections = 0

    # -- identity --------------------------------------------------------
    @property
    def address(self) -> str:
        return self.identity.address

    # -- Ledger Services -------------------------------------------------
    def install(self, contract: SmartContract) -> None:
        """Install a natively-provided Ledger Service."""
        self.registry.register(contract)

    # -- deterministic execution -----------------------------------------
    def execute_batch(self, calls: List[Dict[str, Any]],
                      base_state: Optional[Dict[str, Dict[str, Any]]] = None,
                      install: bool = False
                      ) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]],
                                 List[Dict[str, Any]]]:
        """Execute a batch of service calls against a *copy* of the state.

        Returns (new_state_dict, outcomes, events).  Nothing is committed: the
        caller decides what to do with the result, which is how the same code
        path can serve both block proposal and independent validation.

        `install` is True only on the commit path, so that a DEPLOY_CONTRACT
        transaction that is merely being *evaluated* never leaves a new Ledger
        Service behind in the registry.
        """
        work = WorldState(copy.deepcopy(base_state if base_state is not None
                                        else self.state.snapshot()))
        outcomes: List[Dict[str, Any]] = []
        events: List[Dict[str, Any]] = []

        for entry in calls:
            call = entry["call"]
            op = call.get("op", "APPEND")
            if op == "DEPLOY_CONTRACT":
                outcome = self._execute_deploy(work, call, install)
            else:
                outcome = self._execute_service_call(work, call)
            outcomes.append(outcome)
            for ev in outcome.get("events", []):
                events.append({"service": call["service"], "name": ev["name"],
                               "payload": ev["payload"]})
        return work.as_dict(), outcomes, events

    def _execute_service_call(self, work: WorldState,
                              call: Dict[str, Any]) -> Dict[str, Any]:
        contract = self.registry.get(call["service"])
        if contract is None:
            return {"call_id": call["id"], "ok": False,
                    "reason": f"no such Ledger Service {call['service']}",
                    "result": {}, "events": []}
        sub_state = work.service(call["service"])
        try:
            res: ContractResult = contract.invoke(
                sub_state, call["endpoint"], call.get("args", {}),
                call.get("caller", ""), float(call.get("t", 0.0)))
        except Exception as exc:                      # contract fault
            return {"call_id": call["id"], "ok": False,
                    "reason": f"contract error: {type(exc).__name__}: {exc}",
                    "result": {}, "events": []}
        return {"call_id": call["id"], "ok": res.ok, "reason": res.reason,
                "result": res.result, "events": res.events if res.ok else []}

    def _execute_deploy(self, work: WorldState, call: Dict[str, Any],
                        install: bool) -> Dict[str, Any]:
        """Install or replace a Ledger Service carried by a transaction.

        Paper, S2.3: "even the executable code of Ledger Services can be
        deployed and updated online by means of a distributed ledger
        transaction, just like any other state change."
        """
        args = call.get("args", {})
        name = str(args.get("name", ""))
        source = str(args.get("source", ""))
        try:
            if install:
                self.registry.deploy_source(name, source)
            else:
                compile_contract(source)      # evaluate only, do not install
        except Exception as exc:
            return {"call_id": call["id"], "ok": False,
                    "reason": f"deployment rejected: {exc}",
                    "result": {}, "events": []}
        contracts = work.service("__contracts__")
        contracts[name] = {"source_hash": hash_object(source),
                           "deployed_by": call.get("caller", ""),
                           "t": call.get("t", 0.0),
                           "version": contracts.get(name, {}).get("version", 0) + 1}
        return {"call_id": call["id"], "ok": True, "reason": "",
                "result": {"deployed": name,
                           "version": contracts[name]["version"]},
                "events": [{"name": "ContractDeployed",
                            "payload": {"name": name,
                                        "version": contracts[name]["version"]}}]}

    # -- block proposal ---------------------------------------------------
    def propose(self, calls: List[Dict[str, Any]], op: str,
                timestamp: float) -> Tuple[Block, List[Dict[str, Any]],
                                           List[Dict[str, Any]]]:
        with self._lock:
            head = self.chain.head
            new_state, outcomes, events = self.execute_batch(calls)
            enriched = [dict(c, outcome=o) for c, o in zip(calls, outcomes)]
            block = Block(index=head.index + 1, prev_hash=head.hash,
                          calls=enriched, timestamp=timestamp,
                          proposer=self.address, op=op,
                          state_root=hash_object(new_state))
            if self.byzantine:
                # A Byzantine proposer advertises a state root that does not
                # correspond to the honest execution of its own block.
                block.state_root = hash_object({"forged": block.index})
            block.seal()
            block.signature = self.identity.sign(block.header())
            return block, outcomes, events

    # -- validation -------------------------------------------------------
    def validate(self, block: Block, directory: KeyDirectory) -> Vote:
        """Independently re-execute the block and vote on its validity."""
        with self._lock:
            self.validations += 1

            if not directory.verify_signed(block.proposer, block.header(),
                                           block.signature):
                return self._vote(False, "", "invalid proposer signature")
            if block.hash != block.compute_hash():
                return self._vote(False, "", "block hash does not match header")
            if block.merkle != block.compute_merkle():
                return self._vote(False, "", "merkle root does not match calls")
            if block.prev_hash != self.chain.head.hash:
                return self._vote(False, "", "block does not extend my head")

            for entry in block.calls:
                call = entry["call"]
                if not directory.verify_signed(call.get("caller", ""),
                                               call, entry.get("signature", "")):
                    return self._vote(False, "", f"bad client signature on {call['id']}")

            new_state, outcomes, _ = self.execute_batch(
                [{"call": e["call"], "signature": e.get("signature", "")}
                 for e in block.calls])
            local_root = hash_object(new_state)

            for mine, theirs in zip(outcomes, [e["outcome"] for e in block.calls]):
                if mine["ok"] != theirs["ok"]:
                    return self._vote(False, local_root,
                                      f"outcome mismatch on {mine['call_id']}")
            if local_root != block.state_root:
                return self._vote(False, local_root, "state root mismatch")
            return self._vote(True, local_root, "")

    def _vote(self, accept: bool, state_root: str, reason: str) -> Vote:
        if self.byzantine:
            # A Byzantine validator inverts its verdict.
            accept = not accept
            reason = ("byzantine inversion: " + reason) if reason else "byzantine inversion"
        if not accept:
            self.rejections += 1
        vote = Vote(peer=self.address, accept=accept, state_root=state_root,
                    reason=reason)
        vote.signature = self.identity.sign(
            {"peer": vote.peer, "accept": vote.accept,
             "state_root": vote.state_root})
        return vote

    # -- commit -----------------------------------------------------------
    def commit(self, block: Block) -> List[Dict[str, Any]]:
        """Append an agreed block and apply its effects to the replica."""
        with self._lock:
            new_state, _, events = self.execute_batch(
                [{"call": e["call"], "signature": e.get("signature", "")}
                 for e in block.calls], install=True)
            self.chain.append(block)
            self.state.replace(new_state)
            return events

    # -- read path (no consensus needed) ----------------------------------
    def read(self, service: str, key: str, default: Any = None) -> Any:
        with self._lock:
            return copy.deepcopy(self.state.read(service, key, default))

    def service_state(self, service: str) -> Dict[str, Any]:
        with self._lock:
            return copy.deepcopy(self.state.service(service))

    # -- recovery ---------------------------------------------------------
    def sync_from(self, other: "PeerNode") -> int:
        """Catch up with a peer after a crash fault.

        "automatically kept in-sync across all instances, using peer-to-peer
        communication only" (paper S3.3).
        """
        with self._lock:
            applied = 0
            for block in other.chain.blocks[self.chain.height + 1:]:
                self.commit(copy.deepcopy(block))
                applied += 1
            return applied

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        flags = []
        if not self.online:
            flags.append("OFFLINE")
        if self.byzantine:
            flags.append("BYZANTINE")
        return (f"<PeerNode {self.node_id} h={self.chain.height} "
                f"{' '.join(flags)}>")
