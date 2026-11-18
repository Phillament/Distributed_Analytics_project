"""Synchronization Services Component (Ledger Tier).

Paper, S3.3: "The Synchronization Services Component includes all the smart
contracts that support the Digital Models XC Functions. ... they are used to
keep digital models in-synch with changes in the real world they represent.
As for all Ledger Services, they expose one or more service endpoints for
clients to call upon changes in the real world."

The paper's own motivating example (S2.2) is implemented here:

    "For instance, a machine model assumes a given value of electric power /
     energy consumption, but the actual values will diverge as the real
     machine wears down.  To detect this gap and correct the model
     accordingly, raw data from the Field (direct) or complex analysis
     algorithms (from Analytics) can be used."

So: Edge Gateways report *observed* parameter values; this contract compares
them against the model, and when the relative gap exceeds the declared
threshold it corrects the model (exponentially weighted update), bumps the
model version and emits ModelUpdated.  The Simulation Domain is a pure
consumer of the resulting models -- "Model synchronization functionality is
not part of the Simulation domain, which acts only as a consumer of the
Digital Models XC Functions" (S2.2).
"""
from __future__ import annotations

from typing import Callable, Dict

from ..smart_contract import ContractResult, SmartContract

DEFAULT_THRESHOLD = 0.05      # 5 % relative gap triggers a correction
DEFAULT_ALPHA = 0.5           # EWMA weight given to the observation


class SynchronizationServices(SmartContract):
    name = "SynchronizationServices"
    domain = "SIMULATION"
    xc = "DIGITAL_MODELS"
    description = ("Keeps digital models in sync with the real world by "
                   "detecting and correcting model drift")

    def endpoints(self) -> Dict[str, Callable[..., ContractResult]]:
        return {
            "registerModel": self.register_model,
            "reportObservation": self.report_observation,
            "setThreshold": self.set_threshold,
        }

    # -- model lifecycle --------------------------------------------------
    def register_model(self, state, args, caller, t) -> ContractResult:
        """Publish a digital model of a real-world entity."""
        model_id = str(args["model_id"])
        models = state.setdefault("models", {})
        models[model_id] = {
            "entity": args.get("entity", model_id),
            "kind": args.get("kind", "MACHINE"),
            "params": dict(args.get("params", {})),
            "thresholds": dict(args.get("thresholds", {})),
            "alpha": float(args.get("alpha", DEFAULT_ALPHA)),
            "version": 1,
            "last_sync_t": t,
            "sync_count": 0,
            "drift": {},
        }
        res = ContractResult(result={"model_id": model_id, "version": 1})
        res.emit("ModelRegistered", model_id=model_id,
                 params=models[model_id]["params"])
        return res

    def set_threshold(self, state, args, caller, t) -> ContractResult:
        model_id = str(args["model_id"])
        model = state.setdefault("models", {}).get(model_id)
        if not model:
            return ContractResult.reject(f"unknown model {model_id}")
        model["thresholds"][str(args["param"])] = float(args["threshold"])
        return ContractResult(result={"model_id": model_id,
                                      "param": args["param"],
                                      "threshold": args["threshold"]})

    # -- real-to-digital synchronisation ----------------------------------
    def report_observation(self, state, args, caller, t) -> ContractResult:
        """Report a measured value of a model parameter.

        Returns, and records, the relative drift.  The model is corrected only
        if the drift exceeds the parameter's threshold -- the paper warns that
        "the ideal clients perform frequent reads and infrequent writes of the
        shared state" (S3.3), so a dead band is the right behaviour: frequent
        observations, rare model updates.
        """
        model_id = str(args["model_id"])
        param = str(args["param"])
        observed = float(args["observed"])
        n = int(args.get("n", 1))

        models = state.setdefault("models", {})
        model = models.get(model_id)
        if not model:
            return ContractResult.reject(f"unknown model {model_id}")
        if param not in model["params"]:
            return ContractResult.reject(
                f"model {model_id} has no parameter {param}")

        modelled = float(model["params"][param])
        denom = abs(modelled) if abs(modelled) > 1e-12 else 1.0
        drift = (observed - modelled) / denom
        model["drift"][param] = {"relative": drift, "observed": observed,
                                 "modelled": modelled, "n": n, "t": t}
        state["observation_count"] = state.get("observation_count", 0) + 1

        threshold = float(model["thresholds"].get(param, DEFAULT_THRESHOLD))
        res = ContractResult(result={"model_id": model_id, "param": param,
                                     "drift": drift, "threshold": threshold,
                                     "updated": False})
        if abs(drift) <= threshold:
            res.emit("ObservationRecorded", model_id=model_id, param=param,
                     drift=drift)
            return res

        alpha = float(model.get("alpha", DEFAULT_ALPHA))
        corrected = (1.0 - alpha) * modelled + alpha * observed
        model["params"][param] = corrected
        model["version"] += 1
        model["last_sync_t"] = t
        model["sync_count"] += 1
        state["correction_count"] = state.get("correction_count", 0) + 1
        res.result.update({"updated": True, "new_value": corrected,
                           "version": model["version"]})
        res.emit("ModelUpdated", model_id=model_id, param=param,
                 previous=modelled, value=corrected, drift=drift,
                 version=model["version"])
        return res
