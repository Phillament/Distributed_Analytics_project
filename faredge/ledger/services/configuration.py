"""Configuration Services Component (Ledger Tier).

Paper, S3.3: "The Configuration Services Component includes all the smart
contracts supporting the Automation Domain functionalities related to
self-adjustment and reconfiguration, and those related to the management of a
global asset/service registry in the Management XC Functions."

Two capabilities, therefore:

  * a *global* asset/service registry -- the enabler of the "plug-and-play
    factory equipment, which allows faster and less expensive adjustments of
    the production process" promised by the Automation Domain (S2.2).  A
    device that joins any Edge Gateway becomes visible to the whole platform
    through one consensus-backed registry, with no central master.

  * self-adjustment and reconfiguration -- versioned process parameters.  A
    parameter change is a transaction; when it commits, a ledger event is
    emitted and every subscribed Edge Gateway reconfigures itself.  Because
    the write is atomic and majority-approved, all gateways converge on the
    same parameter set.
"""
from __future__ import annotations

from typing import Callable, Dict

from ..smart_contract import ContractResult, SmartContract


class ConfigurationServices(SmartContract):
    name = "ConfigurationServices"
    domain = "AUTOMATION"
    xc = "MANAGEMENT"
    description = ("Global asset/service registry plus versioned, "
                   "consensus-approved process reconfiguration")

    def endpoints(self) -> Dict[str, Callable[..., ContractResult]]:
        return {
            "registerAsset": self.register_asset,
            "deregisterAsset": self.deregister_asset,
            "setParameter": self.set_parameter,
            "publishRecipe": self.publish_recipe,
            "applyRecipe": self.apply_recipe,
            "setLimit": self.set_limit,
        }

    # -- global asset / service registry (Management XC) ------------------
    def register_asset(self, state, args, caller, t) -> ContractResult:
        """Plug-and-play registration of an Edge Node or an edge service."""
        asset_id = str(args["asset_id"])
        registry = state.setdefault("registry", {})
        descriptor = {
            "type": args.get("type", "UNKNOWN"),
            "station": args.get("station", ""),
            "gateway": args.get("gateway", ""),
            "protocol": args.get("protocol", ""),
            "endpoint": args.get("endpoint", ""),
            "signals": list(args.get("signals", [])),
            "capabilities": list(args.get("capabilities", [])),
            "status": "ACTIVE",
            "registered_by": caller,
            "registered_t": t,
        }
        is_new = asset_id not in registry
        registry[asset_id] = descriptor
        state["registry_version"] = state.get("registry_version", 0) + 1
        res = ContractResult(result={"asset_id": asset_id, "new": is_new,
                                     "registry_version": state["registry_version"]})
        res.emit("AssetRegistered", asset_id=asset_id,
                 type=descriptor["type"], station=descriptor["station"],
                 new=is_new)
        return res

    def deregister_asset(self, state, args, caller, t) -> ContractResult:
        asset_id = str(args["asset_id"])
        registry = state.setdefault("registry", {})
        if asset_id not in registry:
            return ContractResult.reject(f"unknown asset {asset_id}")
        registry[asset_id]["status"] = "DECOMMISSIONED"
        registry[asset_id]["decommissioned_t"] = t
        state["registry_version"] = state.get("registry_version", 0) + 1
        res = ContractResult(result={"asset_id": asset_id})
        res.emit("AssetDeregistered", asset_id=asset_id)
        return res

    # -- self-adjustment and reconfiguration ------------------------------
    def set_parameter(self, state, args, caller, t) -> ContractResult:
        """Change one process parameter of one station, by consensus.

        Business rules enforced here keep a misbehaving or stale client from
        driving the line out of its safe envelope -- the ledger is the single
        point where such rules are enforced for every Edge Gateway at once.
        """
        station = str(args["station"])
        key = str(args["key"])
        value = args["value"]
        limits = state.setdefault("limits", {})
        bound = limits.get(key)
        if bound and isinstance(value, (int, float)):
            low, high = bound["min"], bound["max"]
            if not (low <= float(value) <= high):
                return ContractResult.reject(
                    f"{key}={value} outside allowed range [{low}, {high}]")

        params = state.setdefault("parameters", {}).setdefault(station, {})
        previous = params.get(key)
        params[key] = value
        version = state.get("config_version", 0) + 1
        state["config_version"] = version
        history = state.setdefault("history", [])
        history.append({"station": station, "key": key, "from": previous,
                        "to": value, "by": caller, "t": t, "version": version,
                        "reason": args.get("reason", "")})
        res = ContractResult(result={"station": station, "key": key,
                                     "value": value, "config_version": version})
        res.emit("ParameterChanged", station=station, key=key,
                 previous=previous, value=value, config_version=version,
                 reason=args.get("reason", ""))
        return res

    def publish_recipe(self, state, args, caller, t) -> ContractResult:
        recipe_id = str(args["recipe_id"])
        recipes = state.setdefault("recipes", {})
        recipes[recipe_id] = {"parameters": dict(args.get("parameters", {})),
                              "published_by": caller, "t": t}
        res = ContractResult(result={"recipe_id": recipe_id})
        res.emit("RecipePublished", recipe_id=recipe_id)
        return res

    def apply_recipe(self, state, args, caller, t) -> ContractResult:
        """Apply every parameter of a recipe to a station in one transaction."""
        recipe_id = str(args["recipe_id"])
        station = str(args["station"])
        recipe = state.setdefault("recipes", {}).get(recipe_id)
        if not recipe:
            return ContractResult.reject(f"unknown recipe {recipe_id}")
        params = state.setdefault("parameters", {}).setdefault(station, {})
        for key, value in recipe["parameters"].items():
            params[key] = value
        version = state.get("config_version", 0) + 1
        state["config_version"] = version
        res = ContractResult(result={"station": station, "recipe_id": recipe_id,
                                     "config_version": version})
        res.emit("RecipeApplied", station=station, recipe_id=recipe_id,
                 parameters=recipe["parameters"], config_version=version)
        return res

    # -- safety envelope --------------------------------------------------
    def set_limit(self, state, args, caller, t) -> ContractResult:
        """Declare the allowed range of a process parameter.

        The envelope itself is shared state, so the rule that `setParameter`
        enforces is agreed by consensus rather than hard-coded in any node.
        """
        key = str(args["key"])
        low, high = float(args["min"]), float(args["max"])
        if low > high:
            return ContractResult.reject(f"empty range for {key}")
        state.setdefault("limits", {})[key] = {"min": low, "max": high}
        res = ContractResult(result={"key": key, "min": low, "max": high})
        res.emit("LimitDeclared", key=key, min=low, max=high)
        return res
