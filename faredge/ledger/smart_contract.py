"""Smart-contract machinery of the Ledger Tier.

Paper, S2.3: "The logic for assessing the validity of transactions is itself
defined by consensus of all stakeholders: it is a smart contract -- i.e.,
executable code that implements a deterministic algorithm -- deployed on the
ledger.  This means that the coordination of distributed processes is not only
decentralized, but also dynamically (re)definable according to changing
needs."  And: "even the executable code of Ledger Services can be deployed and
updated online by means of a distributed ledger transaction, just like any
other state change."

Two consequences are implemented here:

1.  `SmartContract` is a *deterministic* function of (state, call).  It is
    forbidden from reading the wall clock or any source of randomness; the
    call's own timestamp is passed in explicitly.  Determinism is what allows
    every Peer Node to execute the same call in parallel and arrive at the
    same state root.

2.  `ContractRegistry.deploy_source` installs a contract from source code
    carried by a DEPLOY_CONTRACT transaction, in a restricted namespace.  The
    source lives in the ledger state, so a Peer Node that joins later rebuilds
    exactly the same set of Ledger Services.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

CONTRACT_SOURCE_KEY = "__source__"
CONTRACT_SERVICE = "__contracts__"


@dataclass
class ContractResult:
    """Outcome of executing one service call against the shared state."""

    ok: bool = True
    result: Dict[str, Any] = field(default_factory=dict)
    events: List[Dict[str, Any]] = field(default_factory=list)
    reason: str = ""

    def emit(self, name: str, **payload: Any) -> None:
        self.events.append({"name": name, "payload": payload})

    @classmethod
    def reject(cls, reason: str) -> "ContractResult":
        return cls(ok=False, reason=reason)


class SmartContract:
    """Base class for a Ledger Services Component.

    Subclasses declare `name`, the RA elements they realise, and a set of
    endpoints.  `invoke` dispatches a call; each endpoint receives the mutable
    sub-state of its own service plus the call arguments, and returns a
    ContractResult.
    """

    name: str = "AbstractContract"
    domain: str = ""        # Functional Domain realised (paper S2.2)
    xc: str = ""            # Crosscutting Function realised, if any
    description: str = ""

    def endpoints(self) -> Dict[str, Callable[..., ContractResult]]:
        raise NotImplementedError

    # -- validation ------------------------------------------------------
    def validate(self, state: Dict[str, Any], endpoint: str,
                 args: Dict[str, Any], caller: str) -> Optional[str]:
        """Business-rule pre-check.  Return an error string to reject.

        This is the hook that makes a Ledger Service a *rule enforcer*:
        "Ledger services in FAR-EDGE will be responsible for sharing process
        state and enforcing business rules across the computing nodes of a
        distributed system" (paper S1.3).
        """
        if endpoint not in self.endpoints():
            return f"unknown endpoint {self.name}.{endpoint}"
        return None

    # -- execution -------------------------------------------------------
    def invoke(self, state: Dict[str, Any], endpoint: str,
               args: Dict[str, Any], caller: str, t: float) -> ContractResult:
        error = self.validate(state, endpoint, args, caller)
        if error:
            return ContractResult.reject(error)
        fn = self.endpoints()[endpoint]
        return fn(state=state, args=args, caller=caller, t=t)

    def endpoint_names(self) -> List[str]:
        return sorted(self.endpoints().keys())


# --------------------------------------------------------------------------
# Dynamic deployment
# --------------------------------------------------------------------------
_SAFE_BUILTINS = {
    "abs": abs, "min": min, "max": max, "sum": sum, "len": len,
    "round": round, "sorted": sorted, "float": float, "int": int,
    "str": str, "bool": bool, "dict": dict, "list": list, "set": set,
    "tuple": tuple, "enumerate": enumerate, "range": range, "zip": zip,
    "any": any, "all": all, "isinstance": isinstance, "ValueError": ValueError,
    "KeyError": KeyError, "True": True, "False": False, "None": None,
}


class ContractDeploymentError(RuntimeError):
    pass


def compile_contract(source: str) -> SmartContract:
    """Compile contract source into a SmartContract instance.

    The namespace deliberately excludes `__import__`, `open`, `eval` and the
    wall clock, so that a deployed Ledger Service cannot become
    non-deterministic or reach outside the ledger.
    """
    namespace: Dict[str, Any] = {
        "__builtins__": dict(_SAFE_BUILTINS),
        "SmartContract": SmartContract,
        "ContractResult": ContractResult,
    }
    try:
        exec(compile(source, "<ledger-contract>", "exec"), namespace)
    except Exception as exc:  # pragma: no cover - deployment failure path
        raise ContractDeploymentError(f"compile failed: {exc}") from exc

    candidates = [
        v for k, v in namespace.items()
        if isinstance(v, type) and issubclass(v, SmartContract) and v is not SmartContract
    ]
    if not candidates:
        raise ContractDeploymentError("source defines no SmartContract subclass")
    return candidates[-1]()


class ContractRegistry:
    """The set of Ledger Services known to one Peer Node.

    Every Peer Node owns its own registry instance; because deployment travels
    as a ledger transaction, all registries converge.
    """

    def __init__(self) -> None:
        self._contracts: Dict[str, SmartContract] = {}
        self._sources: Dict[str, str] = {}

    def register(self, contract: SmartContract) -> None:
        self._contracts[contract.name] = contract

    def deploy_source(self, name: str, source: str) -> SmartContract:
        contract = compile_contract(source)
        contract.name = name
        self._contracts[name] = contract
        self._sources[name] = source
        return contract

    def get(self, name: str) -> Optional[SmartContract]:
        return self._contracts.get(name)

    def names(self) -> List[str]:
        return sorted(self._contracts)

    def sources(self) -> Dict[str, str]:
        return dict(self._sources)

    def __contains__(self, name: str) -> bool:
        return name in self._contracts

    def __len__(self) -> int:
        return len(self._contracts)
