"""Data Publishing Services Component (Ledger Tier).

This contract implements, essentially verbatim, the worked example the paper
gives in S3.3:

    "The Data Publishing Services Component includes all the smart contracts
     that support the Analytics Domain functionalities ... they are used to
     collect/aggregate results from ad-hoc distributed analytics processes
     running on EGs.  They expose service endpoints for clients to call on
     need i.e. the availability of new or updated local results on any EG,
     that must be contributed to the global view in order to keep it current.
     For instance, a standard set of Key Performance Indicators (KPI) may be
     measured locally on each EG, so that raw Field data is not transmitted on
     the network beyond the narrow boundaries of a plant's LAN.  Each time a
     local KPI value changes, this change is submitted to the relevant Data
     Publishing service endpoint and the underlying smart contract will
     compute the new value of the corresponding global KPI."

Note the last sentence: the aggregation logic lives *in the Ledger Service*,
not in the Cloud and not on any single Edge Gateway.  That is what this
contract does.
"""
from __future__ import annotations

from typing import Any, Callable, Dict

from ..smart_contract import ContractResult, SmartContract

# Supported aggregation methods.  All are associative/commutative so that the
# global value is independent of the order in which local results arrive --
# a necessary property, because transaction order across Edge Gateways is not
# controlled by any single node.
AGG_SUM = "sum"
AGG_WEIGHTED_MEAN = "weighted_mean"
AGG_MIN = "min"
AGG_MAX = "max"
AGG_RATIO = "ratio"        # global = sum(numerators) / sum(denominators)

METHODS = (AGG_SUM, AGG_WEIGHTED_MEAN, AGG_MIN, AGG_MAX, AGG_RATIO)


class DataPublishingServices(SmartContract):
    name = "DataPublishingServices"
    domain = "ANALYTICS"
    description = ("Collects local analytics results from Edge Gateways and "
                   "computes the corresponding global KPIs by consensus")

    def endpoints(self) -> Dict[str, Callable[..., ContractResult]]:
        return {
            "declareKPI": self.declare_kpi,
            "submitLocalKPI": self.submit_local_kpi,
            "resetKPI": self.reset_kpi,
        }

    # -- declaration ------------------------------------------------------
    def declare_kpi(self, state, args, caller, t) -> ContractResult:
        """Declare a KPI and how local contributions combine into a global."""
        kpi = str(args["kpi"])
        method = str(args.get("method", AGG_WEIGHTED_MEAN))
        if method not in METHODS:
            return ContractResult.reject(f"unsupported aggregation {method}")
        defs = state.setdefault("definitions", {})
        defs[kpi] = {"method": method,
                     "unit": args.get("unit", ""),
                     "description": args.get("description", "")}
        state.setdefault("local", {})
        state.setdefault("global", {})
        state.setdefault("submission_count", 0)
        res = ContractResult(result={"kpi": kpi, "method": method})
        res.emit("KPIDeclared", kpi=kpi, method=method)
        return res

    # -- the hot path -----------------------------------------------------
    def submit_local_kpi(self, state, args, caller, t) -> ContractResult:
        """Contribute a local result and recompute the global KPI.

        args:
            eg          -- identifier of the contributing Edge Gateway
            kpi         -- KPI name, previously declared
            value       -- the local value
            weight      -- weight for weighted_mean (e.g. #samples, #parts)
            numerator   -- for the ratio method
            denominator -- for the ratio method
        """
        kpi = str(args["kpi"])
        eg = str(args["eg"])
        defs = state.setdefault("definitions", {})
        if kpi not in defs:
            return ContractResult.reject(f"KPI {kpi} not declared")

        method = defs[kpi]["method"]
        local = state.setdefault("local", {}).setdefault(kpi, {})
        entry: Dict[str, Any] = {"t": t, "by": caller}
        if method == AGG_RATIO:
            entry["numerator"] = float(args["numerator"])
            entry["denominator"] = float(args["denominator"])
            entry["value"] = (entry["numerator"] / entry["denominator"]
                              if entry["denominator"] else 0.0)
        else:
            entry["value"] = float(args["value"])
            entry["weight"] = float(args.get("weight", 1.0))
        local[eg] = entry
        state["submission_count"] = state.get("submission_count", 0) + 1

        new_global = self._aggregate(method, local)
        previous = state.setdefault("global", {}).get(kpi, {}).get("value")
        state["global"][kpi] = {
            "value": new_global,
            "method": method,
            "unit": defs[kpi]["unit"],
            "contributors": len(local),
            "t": t,
        }
        res = ContractResult(result={"kpi": kpi, "global": new_global,
                                     "contributors": len(local)})
        res.emit("GlobalKPIUpdated", kpi=kpi, value=new_global,
                 previous=previous, contributors=len(local))
        return res

    def reset_kpi(self, state, args, caller, t) -> ContractResult:
        kpi = str(args["kpi"])
        state.setdefault("local", {}).pop(kpi, None)
        state.setdefault("global", {}).pop(kpi, None)
        res = ContractResult(result={"kpi": kpi, "reset": True})
        res.emit("KPIReset", kpi=kpi)
        return res

    # -- aggregation ------------------------------------------------------
    @staticmethod
    def _aggregate(method: str, local: Dict[str, Dict[str, Any]]) -> float:
        values = [e["value"] for e in local.values()]
        if not values:
            return 0.0
        if method == AGG_SUM:
            return float(sum(values))
        if method == AGG_MIN:
            return float(min(values))
        if method == AGG_MAX:
            return float(max(values))
        if method == AGG_RATIO:
            num = sum(e.get("numerator", 0.0) for e in local.values())
            den = sum(e.get("denominator", 0.0) for e in local.values())
            return float(num / den) if den else 0.0
        # weighted mean
        weights = [e.get("weight", 1.0) for e in local.values()]
        total = sum(weights)
        if total <= 0:
            return float(sum(values) / len(values))
        return float(sum(v * w for v, w in zip(values, weights)) / total)
