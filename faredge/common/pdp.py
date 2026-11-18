"""Policy Decision Point (PDP) -- Security Crosscutting Function.

The PDP appears in three Tiers of the Platform Design, with an explicitly
different resource profile in each:

Field Tier (S3.1): "The Policy Decision Point (PDP) Component implements parts
of the Security XC Functions related to the enforcement of access policies.
It supports Smart Objects in assessing incoming calls against security
policies that are defined and maintained centrally, approving their execution
only if they are legitimate."

Edge Tier (S3.2): "It is not constrained by low-end computing resources, as is
the case with the Field Tier version.  Its users are those service points
exposed by Components and Enablers running on EGs."

Cloud Tier (S3.4): the PDP Component enforces policies while the Security
Management Component administers them.

The implementation mirrors that split: one PDP class whose *cache capacity*
differs by Tier.  A Field-Tier PDP has room for only a couple of policies and
therefore has to fetch from the central Policy Administration Point (the Cloud
Security Management Component) across the network more often -- which is
precisely the trade-off the paper is pointing at, and which the test suite
measures.
"""
from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

PERMIT = "PERMIT"
DENY = "DENY"

# Cache capacity by Tier: the Field Tier version is resource-constrained.
CACHE_CAPACITY = {"FIELD": 2, "EDGE": 64, "CLOUD": 1024}


@dataclass
class Policy:
    """An access-control rule, administered centrally (Security Management)."""

    policy_id: str
    subjects: Tuple[str, ...]      # roles or addresses; ("*",) = any
    actions: Tuple[str, ...]       # e.g. ("set_speed", "stop"); ("*",) = any
    resources: Tuple[str, ...]     # e.g. ("ST1/*",); ("*",) = any
    effect: str = PERMIT
    condition: Optional[Callable[[Dict[str, Any]], bool]] = None

    def matches(self, subject: str, action: str, resource: str) -> bool:
        return (self._match(self.subjects, subject)
                and self._match(self.actions, action)
                and self._match(self.resources, resource))

    @staticmethod
    def _match(patterns: Tuple[str, ...], value: str) -> bool:
        for pattern in patterns:
            if pattern == "*" or pattern == value:
                return True
            if pattern.endswith("/*") and value.startswith(pattern[:-1]):
                return True
        return False


@dataclass
class Decision:
    permitted: bool
    policy_id: str = ""
    reason: str = ""
    cache_hit: bool = False
    fetch_latency_ms: float = 0.0


@dataclass
class PdpStats:
    evaluations: int = 0
    permits: int = 0
    denies: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    fetch_latency_ms: float = 0.0

    def as_dict(self) -> Dict[str, Any]:
        total = max(1, self.cache_hits + self.cache_misses)
        return {"evaluations": self.evaluations, "permits": self.permits,
                "denies": self.denies, "cache_hits": self.cache_hits,
                "cache_misses": self.cache_misses,
                "cache_hit_rate": round(self.cache_hits / total, 4),
                "policy_fetch_latency_ms": round(self.fetch_latency_ms, 2)}


class PolicyAdministrationPoint:
    """Where policies are "defined and maintained centrally".

    Realised by the Cloud Tier's Security Management Component.
    """

    def __init__(self) -> None:
        self._policies: Dict[str, Policy] = {}
        self.version = 0
        self.fetches = 0

    def put(self, policy: Policy) -> None:
        self._policies[policy.policy_id] = policy
        self.version += 1

    def remove(self, policy_id: str) -> None:
        self._policies.pop(policy_id, None)
        self.version += 1

    def applicable(self, subject: str, action: str, resource: str) -> List[Policy]:
        """Fetch the policy set relevant to one request."""
        self.fetches += 1
        return [p for p in self._policies.values()
                if p.matches(subject, action, resource)]

    def all_policies(self) -> List[Policy]:
        return list(self._policies.values())


class PolicyDecisionPoint:
    """Enforcement-side policy evaluation, with a Tier-sized decision cache."""

    def __init__(self, owner_id: str, tier: str,
                 pap: PolicyAdministrationPoint, link=None):
        self.owner_id = owner_id
        self.tier = tier
        self.capacity = CACHE_CAPACITY.get(tier, 16)
        self._pap = pap
        self._link = link                    # link to the central PAP
        self._cache: "OrderedDict[Tuple[str, str, str], Tuple[int, Decision]]" = OrderedDict()
        self.stats = PdpStats()

    @property
    def pap(self) -> PolicyAdministrationPoint:
        """The central Policy Administration Point this PDP enforces for."""
        return self._pap

    # -- the single entry point ------------------------------------------
    def evaluate(self, subject: str, action: str, resource: str,
                 context: Optional[Dict[str, Any]] = None) -> Decision:
        """Assess an incoming call, "approving their execution only if they
        are legitimate"."""
        self.stats.evaluations += 1
        key = (subject, action, resource)
        cached = self._cache.get(key)
        if cached and cached[0] == self._pap.version:
            self._cache.move_to_end(key)
            self.stats.cache_hits += 1
            decision = Decision(cached[1].permitted, cached[1].policy_id,
                                cached[1].reason, cache_hit=True)
            self._tally(decision)
            return decision

        self.stats.cache_misses += 1
        latency = 0.0
        if self._link is not None:
            latency = self._link.transmit(192, "POLICY_FETCH")
        self.stats.fetch_latency_ms += latency

        policies = self._pap.applicable(subject, action, resource)
        decision = self._combine(policies, context or {})
        decision.fetch_latency_ms = latency

        self._cache[key] = (self._pap.version, decision)
        self._cache.move_to_end(key)
        while len(self._cache) > self.capacity:
            self._cache.popitem(last=False)

        self._tally(decision)
        return decision

    # -- deny-overrides combining algorithm ------------------------------
    @staticmethod
    def _combine(policies: List[Policy], context: Dict[str, Any]) -> Decision:
        if not policies:
            return Decision(False, "", "no applicable policy (default deny)")
        permit: Optional[Policy] = None
        for policy in policies:
            if policy.condition is not None and not policy.condition(context):
                continue
            if policy.effect == DENY:
                return Decision(False, policy.policy_id,
                                "explicitly denied by policy")
            permit = permit or policy
        if permit is None:
            return Decision(False, "", "no policy condition satisfied")
        return Decision(True, permit.policy_id, "permitted")

    def _tally(self, decision: Decision) -> None:
        if decision.permitted:
            self.stats.permits += 1
        else:
            self.stats.denies += 1

    def invalidate(self) -> None:
        self._cache.clear()
