"""Edge Automation Services Enabler (Edge Tier).

Paper, S3.2: "The Edge Automation Services Enabler implements the core part of
the Automation Domain, as it defines a runtime environment for local
automation workflows.  The environment provides core services like security
and a local registry of Field device endpoints.  Such services and their
interfaces comply with documented FAR-EDGE Platform standards to allow edge
automation workflows developed on a given Platform to be easily migrated to
another."

The runtime is therefore built around one documented interface --
`WorkflowContext` -- which is the *only* thing a workflow may touch.  A
workflow never imports a gateway, a protocol adapter or a ledger peer, so the
same workflow class runs unchanged on any Edge Gateway, which is exactly the
portability property the paper asks for.

Three workflows ship with the platform:

    StationCycleWorkflow     drives one station's production cycle and
                             coordinates globally through Orchestration
                             Services (distributed automation)
    SafetyReactionWorkflow   closes the detect-react loop locally, which is
                             where the latency benefit of proximity shows
    ReconfigurationWorkflow  applies a parameter change agreed on the ledger
                             (self-adjustment / plug-and-play)
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from ..common.messages import Event, LedgerEvent

# Workflow lifecycle states.
IDLE = "IDLE"
WAITING_FOR_PART = "WAITING_FOR_PART"
PROCESSING = "PROCESSING"
BLOCKED = "BLOCKED"
STOPPED = "STOPPED"


class WorkflowContext:
    """The documented, platform-standard interface offered to workflows.

    Core services, per the paper: security (the Edge-Tier PDP), a local
    registry of Field device endpoints (Field Abstraction) and -- so that a
    *local* workflow can take part in a *distributed* one -- the Ledger
    Clients.
    """

    def __init__(self, gateway_id: str, station: str, field_abstraction,
                 pdp, ledger_clients, logger: Optional[Callable[[str], None]] = None):
        self.gateway_id = gateway_id
        self.station = station
        self._fa = field_abstraction
        self._pdp = pdp
        self.ledger = ledger_clients
        self._logger = logger
        self.blackboard: Dict[str, Any] = {}

    # -- local registry of Field device endpoints ------------------------
    def devices(self, node_type: Optional[str] = None) -> List[Any]:
        found = self._fa.discover(self.station)
        return [e for e in found if node_type is None or e.node_type == node_type]

    def command(self, target: str, action: str,
                args: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self._fa.send_command(target, action, args or {},
                                     issuer=self.gateway_id)

    # -- security --------------------------------------------------------
    def may(self, action: str, resource: str) -> bool:
        if self._pdp is None:
            return True
        return self._pdp.evaluate(self.gateway_id, action, resource).permitted

    def log(self, message: str) -> None:
        if self._logger:
            self._logger(f"[{self.station}] {message}")


class AutomationWorkflow:
    """Base class of a local automation workflow."""

    workflow_id = "abstract"
    description = ""

    def __init__(self) -> None:
        self.state = IDLE
        self.ticks = 0
        self.errors: List[str] = []

    def on_start(self, ctx: WorkflowContext) -> None:
        pass

    def step(self, ctx: WorkflowContext) -> None:
        pass

    def on_field_event(self, ctx: WorkflowContext, event: Event) -> None:
        pass

    def on_ledger_event(self, ctx: WorkflowContext, event: LedgerEvent) -> None:
        pass

    def on_stop(self, ctx: WorkflowContext) -> None:
        pass


# --------------------------------------------------------------------------
# Distributed automation: one station's share of a line-wide workflow
# --------------------------------------------------------------------------
class StationCycleWorkflow(AutomationWorkflow):
    """Drive one station and coordinate with the rest of the line.

    Every instance runs on its own Edge Gateway and knows nothing about the
    other instances; all coordination goes through Orchestration Services on
    the Ledger Tier.  This is the paper's answer to running "multiple parallel
    instances, each focused on its own narrow scope" without "any centralized
    service being in charge of it" (S2.3, S3.3).
    """

    workflow_id = "station_cycle"
    description = "Station production cycle, coordinated through the ledger"

    def __init__(self, station: str, work_order: str, is_first_station: bool):
        super().__init__()
        self.station = station
        self.work_order = work_order
        self.is_first_station = is_first_station
        self.current_part: Optional[str] = None
        self.lock_held = False
        self.parts_processed = 0
        self.parts_scrapped = 0
        self.claims_refused = 0
        self.reports_refused = 0

    # -- lifecycle -------------------------------------------------------
    def on_start(self, ctx: WorkflowContext) -> None:
        """Take exclusive control of the station, by consensus."""
        receipt = ctx.ledger.orchestration.acquire_station(self.station,
                                                           ctx.gateway_id)
        self.lock_held = receipt.accepted
        self.state = WAITING_FOR_PART if receipt.accepted else BLOCKED
        if not receipt.accepted:
            self.errors.append(f"could not acquire {self.station}: {receipt.reason}")
        ctx.log(f"station lock {'acquired' if receipt.accepted else 'refused'}")

    def on_stop(self, ctx: WorkflowContext) -> None:
        if self.lock_held:
            ctx.ledger.orchestration.release_station(self.station, ctx.gateway_id)
            self.lock_held = False
        self.state = STOPPED

    # -- the cycle -------------------------------------------------------
    def claim(self, ctx: WorkflowContext) -> Optional[str]:
        """Claim the next part allowed by the global routing rules."""
        if not self.lock_held:
            return None
        receipt = ctx.ledger.orchestration.claim_part(
            self.station, ctx.gateway_id,
            self.work_order if self.is_first_station else "")
        if not receipt.accepted:
            self.claims_refused += 1
            self.state = WAITING_FOR_PART
            return None
        self.current_part = (receipt.result or {}).get("part_id")
        self.state = PROCESSING
        return self.current_part

    def complete(self, ctx: WorkflowContext, cycle_time_s: float,
                 ok: bool = True) -> bool:
        """Report the outcome of the operation to the whole platform."""
        if not self.current_part:
            return False
        receipt = ctx.ledger.orchestration.report_operation_complete(
            self.current_part, self.station, ctx.gateway_id, cycle_time_s, ok)
        if not receipt.accepted:
            self.reports_refused += 1
            self.errors.append(f"report refused: {receipt.reason}")
            return False
        self.parts_processed += 1
        if not ok:
            self.parts_scrapped += 1
        self.current_part = None
        self.state = WAITING_FOR_PART
        return True

    def on_field_event(self, ctx: WorkflowContext, event: Event) -> None:
        """A part finished on the machine: claim it and report it.

        The Field tells us *when* an operation completed; the ledger decides
        *whether* that is legal and makes the fact globally visible.
        """
        self.ticks += 1
        if event.name != "OperationFinished":
            return
        if self.current_part is None and self.claim(ctx) is None:
            return
        self.complete(ctx, float(event.payload.get("cycle_time_s", 0.0)),
                      bool(event.payload.get("ok", True)))

    def stats(self) -> Dict[str, Any]:
        return {"workflow": self.workflow_id, "station": self.station,
                "state": self.state, "lock_held": self.lock_held,
                "parts_processed": self.parts_processed,
                "parts_scrapped": self.parts_scrapped,
                "claims_refused": self.claims_refused,
                "reports_refused": self.reports_refused}


# --------------------------------------------------------------------------
# Automation driven by Analytics, locally
# --------------------------------------------------------------------------
class SafetyReactionWorkflow(AutomationWorkflow):
    """React to a locally detected anomaly without leaving the Edge Tier.

    "In some advanced scenarios, Automation is controlled -- to some extent --
    by the results of Analytics and/or Simulation" (S2.2).  Here the Edge
    Analytics Engine detects degradation and this workflow throttles the
    station, all inside the gateway: the measured reaction latency is the
    quantity experiment 3 compares against a cloud round trip.
    """

    workflow_id = "safety_reaction"
    description = "Throttle or stop a station on a locally detected anomaly"

    def __init__(self, station: str, actuator_id: str,
                 throttle_factor: float = 0.7):
        super().__init__()
        self.station = station
        self.actuator_id = actuator_id
        self.throttle_factor = throttle_factor
        self.reactions = 0
        self.reaction_latencies_ms: List[float] = []

    def on_field_event(self, ctx: WorkflowContext, event: Event) -> None:
        if event.name not in ("AnomalyDetected", "ThresholdExceeded",
                              "LocalInterlockTrip"):
            return
        started = time.perf_counter()
        action = "stop" if event.severity == "ALARM" else "set_speed"
        args = ({} if action == "stop"
                else {"factor": self.throttle_factor,
                      "cause": event.payload.get("signal", "")})
        result = ctx.command(self.actuator_id, action, args)
        elapsed = (time.perf_counter() - started) * 1000.0
        if result.get("ok"):
            self.reactions += 1
            self.reaction_latencies_ms.append(elapsed)
            ctx.log(f"local reaction {action} in {elapsed:.2f} ms")

    def stats(self) -> Dict[str, Any]:
        lat = self.reaction_latencies_ms
        return {"workflow": self.workflow_id, "reactions": self.reactions,
                "mean_reaction_ms": round(sum(lat) / len(lat), 3) if lat else 0.0,
                "p95_reaction_ms": round(sorted(lat)[int(0.95 * (len(lat) - 1))], 3)
                if lat else 0.0}


# --------------------------------------------------------------------------
# Self-adjustment driven by the ledger
# --------------------------------------------------------------------------
class ReconfigurationWorkflow(AutomationWorkflow):
    """Apply a configuration change that the ledger has approved.

    Supports the "automated configuration of physical production processes ...
    the enabler of plug-and-play factory equipment" (S2.2) and the
    "self-adjustment and reconfiguration" scope of Configuration Services
    (S3.3).  Every gateway subscribed to ParameterChanged converges on the same
    setpoints without any central push.
    """

    workflow_id = "reconfiguration"
    description = "Apply ledger-approved parameter changes to the Field"

    def __init__(self, station: str, actuator_id: str):
        super().__init__()
        self.station = station
        self.actuator_id = actuator_id
        self.applied: Dict[str, Any] = {}
        self.applications = 0
        self.ignored = 0

    def on_ledger_event(self, ctx: WorkflowContext, event: LedgerEvent) -> None:
        if event.name not in ("ParameterChanged", "RecipeApplied"):
            return
        if event.payload.get("station") != self.station:
            self.ignored += 1
            return
        if event.name == "ParameterChanged":
            changes = {event.payload["key"]: event.payload["value"]}
        else:
            changes = dict(event.payload.get("parameters", {}))
        result = ctx.command(self.actuator_id, "reconfigure", changes)
        if result.get("ok"):
            self.applied.update(changes)
            self.applications += 1
            ctx.log(f"applied {changes} (config v{event.payload.get('config_version')})")

    def stats(self) -> Dict[str, Any]:
        return {"workflow": self.workflow_id, "applications": self.applications,
                "ignored": self.ignored, "applied": dict(self.applied)}


# --------------------------------------------------------------------------
# The Enabler
# --------------------------------------------------------------------------
class EdgeAutomationServices:
    """Runtime environment for local automation workflows."""

    def __init__(self, gateway_id: str, station: str, field_abstraction,
                 pdp, ledger_clients, logger: Optional[Callable[[str], None]] = None):
        self.gateway_id = gateway_id
        self.station = station
        self.context = WorkflowContext(gateway_id, station, field_abstraction,
                                       pdp, ledger_clients, logger)
        self.workflows: Dict[str, AutomationWorkflow] = {}
        self.field_events = 0
        self.ledger_events = 0

    # -- deployment ------------------------------------------------------
    def deploy(self, workflow: AutomationWorkflow) -> AutomationWorkflow:
        self.workflows[workflow.workflow_id] = workflow
        workflow.on_start(self.context)
        return workflow

    def undeploy(self, workflow_id: str) -> None:
        workflow = self.workflows.pop(workflow_id, None)
        if workflow:
            workflow.on_stop(self.context)

    def stop_all(self) -> None:
        for workflow in list(self.workflows.values()):
            workflow.on_stop(self.context)
        self.workflows.clear()

    # -- event delivery --------------------------------------------------
    def dispatch_field_event(self, event: Event) -> None:
        self.field_events += 1
        for workflow in list(self.workflows.values()):
            workflow.on_field_event(self.context, event)

    def dispatch_ledger_event(self, event: LedgerEvent) -> None:
        self.ledger_events += 1
        for workflow in list(self.workflows.values()):
            workflow.on_ledger_event(self.context, event)

    def tick(self) -> None:
        for workflow in list(self.workflows.values()):
            workflow.step(self.context)

    def get(self, workflow_id: str) -> Optional[AutomationWorkflow]:
        return self.workflows.get(workflow_id)

    def stats(self) -> Dict[str, Any]:
        return {"gateway": self.gateway_id, "station": self.station,
                "field_events": self.field_events,
                "ledger_events": self.ledger_events,
                "workflows": [w.stats() if hasattr(w, "stats")
                              else {"workflow": w.workflow_id}
                              for w in self.workflows.values()]}
