"""Simulation Domain.

Paper, S2.2: "Simulation Domain: Includes functionalities for simulating the
behaviour of physical production processes for the purpose of optimization or
of testing what/if scenarios at minimal cost and without essential impact on
regular shop activities.  Simulation requires digital models of plants and
processes to be in-sync with the real world objects they represent. ... Model
synchronization functionality is not part of the Simulation domain, which acts
only as a consumer of the Digital Models XC Functions."

Two rules follow, and both are enforced by the design of this module:

1.  The simulator is a *pure consumer*.  It never writes to the ledger and
    never corrects a model; it only reads.  Its inputs come from two places:
      * per-machine physical parameters from Synchronization Services
        (the Digital Models XC Functions), and
      * plant-level availability and quality from Data Publishing Services,
        because "The Automation and Simulation domains, when properly
        configured, can both make direct use of the outcome of data analysis
        algorithms" (S2.2).

2.  Because the models are kept in sync with the real world, a simulation run
    with synchronised models must be more accurate than one run against the
    original, as-designed models.  That difference is measurable, and the test
    suite measures it.

The production model itself is a serial line with a bottleneck station, which
is the standard first-order model of a flow line.
"""
from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class StationModel:
    """Digital model of one machine."""

    station: str
    ideal_cycle_time_s: float
    nominal_power_kw: float
    version: int = 1

    def copy_with(self, **overrides: float) -> "StationModel":
        data = {"station": self.station,
                "ideal_cycle_time_s": self.ideal_cycle_time_s,
                "nominal_power_kw": self.nominal_power_kw,
                "version": self.version}
        data.update({k: v for k, v in overrides.items() if k in data})
        return StationModel(**data)


@dataclass
class LineModel:
    """Digital model of the production line."""

    stations: List[StationModel] = field(default_factory=list)
    availability: float = 1.0
    quality: float = 1.0
    source: str = "as-designed"

    @property
    def bottleneck(self) -> Optional[StationModel]:
        return max(self.stations, key=lambda s: s.ideal_cycle_time_s,
                   default=None)


@dataclass
class SimulationResult:
    predicted_throughput_pph: float
    predicted_parts_good: float
    predicted_energy_kwh: float
    predicted_energy_per_part_kwh: float
    bottleneck_station: str
    bottleneck_cycle_time_s: float
    availability: float
    quality: float
    duration_s: float
    model_source: str
    stations: int

    def as_dict(self) -> Dict[str, Any]:
        return {k: (round(v, 6) if isinstance(v, float) else v)
                for k, v in self.__dict__.items()}


class LineSimulator:
    """Simulates the line from the models the Platform maintains."""

    def __init__(self, ledger_clients, stations: List[str]):
        self._ledger = ledger_clients
        self.stations = list(stations)
        self.runs = 0

    # ------------------------------------------------------------------
    # model acquisition -- read-only consumption of Digital Models XC
    # ------------------------------------------------------------------
    def current_model(self, use_synced_models: bool = True,
                      as_designed: Optional[Dict[str, Dict[str, float]]] = None
                      ) -> LineModel:
        """Read the line model.

        With `use_synced_models=True` the parameters are those that
        Synchronization Services has corrected against reality; with False the
        original as-designed values are used, which is the comparison that
        shows what model synchronisation is worth.
        """
        ledger_models = self._ledger.synchronization.models()
        stations: List[StationModel] = []
        for station in self.stations:
            if use_synced_models and station in ledger_models:
                params = ledger_models[station]["params"]
                version = ledger_models[station].get("version", 1)
            elif as_designed and station in as_designed:
                params = as_designed[station]
                version = 0
            elif station in ledger_models:
                params = ledger_models[station]["params"]
                version = ledger_models[station].get("version", 1)
            else:
                continue
            stations.append(StationModel(
                station=station,
                ideal_cycle_time_s=float(params.get("ideal_cycle_time_s", 0.0)),
                nominal_power_kw=float(params.get("nominal_power_kw", 0.0)),
                version=int(version)))

        globals_ = self._ledger.data_publishing.global_kpis()
        availability = float(globals_.get("plant_availability", {}).get("value", 1.0))
        quality = float(globals_.get("plant_quality", {}).get("value", 1.0))
        return LineModel(stations=stations, availability=availability,
                         quality=quality,
                         source="synced" if use_synced_models else "as-designed")

    # ------------------------------------------------------------------
    # the simulation itself
    # ------------------------------------------------------------------
    @staticmethod
    def simulate(model: LineModel, duration_s: float) -> SimulationResult:
        """Serial-line throughput model.

        The slowest station sets the pace of the line; availability and quality
        scale the good-part output; energy is the sum of the machines' draw
        over the time they actually run.
        """
        bottleneck = model.bottleneck
        if bottleneck is None or bottleneck.ideal_cycle_time_s <= 0:
            return SimulationResult(0.0, 0.0, 0.0, 0.0, "", 0.0,
                                    model.availability, model.quality,
                                    duration_s, model.source,
                                    len(model.stations))
        hours = duration_s / 3600.0
        gross_rate_pph = 3600.0 / bottleneck.ideal_cycle_time_s
        good_rate_pph = gross_rate_pph * model.availability * model.quality
        parts_good = good_rate_pph * hours
        installed_kw = sum(s.nominal_power_kw for s in model.stations)
        energy_kwh = installed_kw * hours * model.availability
        return SimulationResult(
            predicted_throughput_pph=good_rate_pph,
            predicted_parts_good=parts_good,
            predicted_energy_kwh=energy_kwh,
            predicted_energy_per_part_kwh=(energy_kwh / parts_good
                                           if parts_good else 0.0),
            bottleneck_station=bottleneck.station,
            bottleneck_cycle_time_s=bottleneck.ideal_cycle_time_s,
            availability=model.availability, quality=model.quality,
            duration_s=duration_s, model_source=model.source,
            stations=len(model.stations))

    def run(self, duration_s: float = 3600.0, use_synced_models: bool = True,
            as_designed: Optional[Dict[str, Dict[str, float]]] = None
            ) -> Dict[str, Any]:
        self.runs += 1
        model = self.current_model(use_synced_models, as_designed)
        return self.simulate(model, duration_s).as_dict()

    # ------------------------------------------------------------------
    # what/if analysis
    # ------------------------------------------------------------------
    def what_if(self, overrides: Dict[str, Dict[str, float]],
                duration_s: float = 3600.0,
                availability: Optional[float] = None,
                quality: Optional[float] = None) -> Dict[str, Any]:
        """Predict the effect of a change without touching the shop floor.

        `overrides` maps a station to the parameters to change, e.g.
        {"ST3": {"ideal_cycle_time_s": 9.0}}.
        """
        self.runs += 1
        baseline = self.current_model(use_synced_models=True)
        scenario = LineModel(
            stations=[s.copy_with(**overrides.get(s.station, {}))
                      for s in baseline.stations],
            availability=baseline.availability if availability is None else availability,
            quality=baseline.quality if quality is None else quality,
            source="what-if")
        before = self.simulate(baseline, duration_s)
        after = self.simulate(scenario, duration_s)
        delta = (after.predicted_throughput_pph - before.predicted_throughput_pph)
        return {
            "overrides": overrides,
            "baseline": before.as_dict(),
            "scenario": after.as_dict(),
            "throughput_delta_pph": round(delta, 6),
            "throughput_change_pct": round(
                100.0 * delta / before.predicted_throughput_pph, 4)
            if before.predicted_throughput_pph else 0.0,
            "bottleneck_moved": before.bottleneck_station != after.bottleneck_station,
            "new_bottleneck": after.bottleneck_station,
        }

    def stats(self) -> Dict[str, Any]:
        return {"runs": self.runs, "stations": len(self.stations)}
