"""Orchestration Services Component (Ledger Tier).

Paper, S3.3: "The Orchestration Services Component includes all the smart
contracts that support the Automation Domain functionalities related to
distributed automation workflows. ... The common trait is that they expose one
or more service endpoints for distributed processes to call on need.
Typically, clients will be local automation workflows executed by EGs".

This contract solves exactly the problem the paper raises in S2.3 as the
motivation for the whole Ledger Tier: when a critical function is run as
"multiple parallel instances, each focused on its own narrow scope ... new
problems arise: keeping global variables in-sync across all local instances
of a given process, reaching a consensus among local instances on a global
truth".

Concretely it provides, for a multi-station production line whose stations are
each driven by their own Edge Gateway:

  * a work-order lifecycle shared by all Edge Gateways,
  * distributed mutual exclusion on each station (the station lock), so that
    two parallel automation workflows can never drive the same station, and
  * enforcement of the global routing sequence of every part, so that no
    station can be skipped or executed twice.

No central orchestrator exists: the global truth is the ledger state, and the
business rules are enforced by consensus among Peer Nodes.
"""
from __future__ import annotations

from typing import Any, Callable, Dict

from ..smart_contract import ContractResult, SmartContract


class OrchestrationServices(SmartContract):
    name = "OrchestrationServices"
    domain = "AUTOMATION"
    description = ("Distributed automation workflows: work orders, station "
                   "mutual exclusion and global part routing")

    def endpoints(self) -> Dict[str, Callable[..., ContractResult]]:
        return {
            "defineRoute": self.define_route,
            "openWorkOrder": self.open_work_order,
            "acquireStation": self.acquire_station,
            "releaseStation": self.release_station,
            "claimPart": self.claim_part,
            "reportOperationComplete": self.report_operation_complete,
            "closeWorkOrder": self.close_work_order,
        }

    # -- configuration of the line ---------------------------------------
    def define_route(self, state, args, caller, t) -> ContractResult:
        """Declare the ordered list of stations a part must visit."""
        route = list(args["route"])
        if not route:
            return ContractResult.reject("empty route")
        state["route"] = route
        state.setdefault("locks", {})
        state.setdefault("work_orders", {})
        state.setdefault("parts", {})
        state.setdefault("counters", {"parts_started": 0, "parts_done": 0,
                                      "parts_scrapped": 0, "operations": 0,
                                      "rule_violations": 0})
        res = ContractResult(result={"route": route})
        res.emit("RouteDefined", route=route)
        return res

    def open_work_order(self, state, args, caller, t) -> ContractResult:
        wo = str(args["wo_id"])
        orders = state.setdefault("work_orders", {})
        if wo in orders:
            return ContractResult.reject(f"work order {wo} already exists")
        orders[wo] = {
            "qty": int(args.get("qty", 0)),
            "recipe": args.get("recipe", "default"),
            "released": 0,
            "completed": 0,
            "scrapped": 0,
            "status": "OPEN",
            "opened_by": caller,
            "opened_t": t,
        }
        res = ContractResult(result={"wo_id": wo})
        res.emit("WorkOrderOpened", wo_id=wo, qty=orders[wo]["qty"])
        return res

    # -- distributed mutual exclusion ------------------------------------
    def acquire_station(self, state, args, caller, t) -> ContractResult:
        """Grant exclusive control of a station to one Edge Gateway.

        This is the canonical "global variable kept in-sync across local
        instances": the grant is atomic because the transaction that writes it
        is atomic, and it is globally true because a majority of Peer Nodes
        validated it.
        """
        station = str(args["station"])
        eg = str(args["eg"])
        locks = state.setdefault("locks", {})
        holder = locks.get(station)
        if holder and holder["eg"] != eg:
            state.setdefault("counters", {}).setdefault("lock_conflicts", 0)
            state["counters"]["lock_conflicts"] += 1
            return ContractResult.reject(
                f"station {station} already held by {holder['eg']}")
        locks[station] = {"eg": eg, "since": t, "caller": caller}
        res = ContractResult(result={"station": station, "eg": eg, "granted": True})
        res.emit("StationAcquired", station=station, eg=eg)
        return res

    def release_station(self, state, args, caller, t) -> ContractResult:
        station = str(args["station"])
        eg = str(args["eg"])
        locks = state.setdefault("locks", {})
        holder = locks.get(station)
        if not holder:
            return ContractResult.reject(f"station {station} is not held")
        if holder["eg"] != eg:
            return ContractResult.reject(
                f"station {station} held by {holder['eg']}, not {eg}")
        del locks[station]
        res = ContractResult(result={"station": station, "released": True})
        res.emit("StationReleased", station=station, eg=eg)
        return res

    # -- global part routing ---------------------------------------------
    def claim_part(self, state, args, caller, t) -> ContractResult:
        """Claim a part for processing at a station.

        Enforces the global routing rule: a part may only be claimed by the
        station that is next in its route.  New parts are released against an
        open work order.
        """
        station = str(args["station"])
        eg = str(args["eg"])
        route = state.get("route", [])
        parts = state.setdefault("parts", {})
        counters = state.setdefault("counters", {})

        if station not in route:
            return ContractResult.reject(f"station {station} is not on the route")
        position = route.index(station)

        locks = state.setdefault("locks", {})
        holder = locks.get(station)
        if not holder or holder["eg"] != eg:
            counters["rule_violations"] = counters.get("rule_violations", 0) + 1
            return ContractResult.reject(
                f"{eg} does not hold the lock on station {station}")

        # First station: release a new part against the work order.
        if position == 0:
            wo = str(args.get("wo_id", ""))
            orders = state.setdefault("work_orders", {})
            order = orders.get(wo)
            if not order or order["status"] != "OPEN":
                return ContractResult.reject(f"work order {wo} not open")
            if order["released"] >= order["qty"]:
                return ContractResult.reject(f"work order {wo} fully released")
            order["released"] += 1
            part_id = f"{wo}-P{order['released']:05d}"
            parts[part_id] = {"wo_id": wo, "position": 0, "status": "IN_PROCESS",
                              "history": [], "held_by": eg, "started_t": t}
            counters["parts_started"] = counters.get("parts_started", 0) + 1
            res = ContractResult(result={"part_id": part_id, "position": 0})
            res.emit("PartReleased", part_id=part_id, station=station, eg=eg)
            return res

        # Downstream station: take the oldest part waiting at this position.
        waiting = sorted(
            (pid for pid, p in parts.items()
             if p["position"] == position and p["status"] == "WAITING"),
        )
        if not waiting:
            return ContractResult.reject(f"no part waiting at station {station}")
        part_id = waiting[0]
        parts[part_id]["status"] = "IN_PROCESS"
        parts[part_id]["held_by"] = eg
        res = ContractResult(result={"part_id": part_id, "position": position})
        res.emit("PartClaimed", part_id=part_id, station=station, eg=eg)
        return res

    def report_operation_complete(self, state, args, caller, t) -> ContractResult:
        """Record the outcome of one operation and advance the part."""
        part_id = str(args["part_id"])
        station = str(args["station"])
        eg = str(args["eg"])
        ok = bool(args.get("ok", True))
        cycle_time_s = float(args.get("cycle_time_s", 0.0))

        parts = state.setdefault("parts", {})
        counters = state.setdefault("counters", {})
        part = parts.get(part_id)
        if not part:
            return ContractResult.reject(f"unknown part {part_id}")
        if part["status"] != "IN_PROCESS" or part.get("held_by") != eg:
            counters["rule_violations"] = counters.get("rule_violations", 0) + 1
            return ContractResult.reject(
                f"part {part_id} is not in process at {eg}")

        route = state.get("route", [])
        if route.index(station) != part["position"]:
            counters["rule_violations"] = counters.get("rule_violations", 0) + 1
            return ContractResult.reject(
                f"part {part_id} is at position {part['position']}, "
                f"not at station {station}")

        part["history"].append({"station": station, "eg": eg, "ok": ok,
                                "cycle_time_s": cycle_time_s, "t": t})
        counters["operations"] = counters.get("operations", 0) + 1
        orders = state.setdefault("work_orders", {})
        order = orders.get(part["wo_id"], {})

        res = ContractResult()
        if not ok:
            part["status"] = "SCRAPPED"
            part["held_by"] = None
            counters["parts_scrapped"] = counters.get("parts_scrapped", 0) + 1
            if order:
                order["scrapped"] = order.get("scrapped", 0) + 1
            res.result = {"part_id": part_id, "status": "SCRAPPED"}
            res.emit("PartScrapped", part_id=part_id, station=station)
            return res

        part["position"] += 1
        part["held_by"] = None
        if part["position"] >= len(route):
            part["status"] = "DONE"
            part["done_t"] = t
            counters["parts_done"] = counters.get("parts_done", 0) + 1
            if order:
                order["completed"] = order.get("completed", 0) + 1
            res.result = {"part_id": part_id, "status": "DONE"}
            res.emit("PartCompleted", part_id=part_id,
                     lead_time_s=t - part["started_t"])
        else:
            part["status"] = "WAITING"
            res.result = {"part_id": part_id, "status": "WAITING",
                          "next_station": route[part["position"]]}
            res.emit("PartForwarded", part_id=part_id,
                     next_station=route[part["position"]])
        return res

    def close_work_order(self, state, args, caller, t) -> ContractResult:
        wo = str(args["wo_id"])
        orders = state.setdefault("work_orders", {})
        if wo not in orders:
            return ContractResult.reject(f"unknown work order {wo}")
        orders[wo]["status"] = "CLOSED"
        orders[wo]["closed_t"] = t
        res = ContractResult(result={"wo_id": wo, "status": "CLOSED"})
        res.emit("WorkOrderClosed", wo_id=wo,
                 completed=orders[wo]["completed"],
                 scrapped=orders[wo].get("scrapped", 0))
        return res
