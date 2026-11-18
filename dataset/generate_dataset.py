"""Synthetic shop-floor dataset generator.

WHY A SYNTHETIC DATASET
-----------------------
The paper (Isaja & Soldatos, IFAC-PapersOnLine 51-11, 2018, 370-375) is a
reference-architecture and platform-design paper.  It reports no experiment,
names no dataset and publishes no numerical results -- its contribution is the
architecture itself.  To exercise that architecture we therefore generate a
dataset that matches the kind of plant the paper targets: a discrete
manufacturing flow line whose stations are each driven by their own Edge
Gateway, instrumented with the signals an industrial edge deployment actually
collects.

WHAT IS GENERATED
-----------------
Per station, a time series sampled at `sample_rate_hz`:

    t                 wall-clock seconds since the start of the shift
    station           station identifier
    vibration_mm_s    machine vibration velocity (ISO 10816 units)
    temperature_c     ambient/bearing temperature
    power_kw          instantaneous electrical power draw
    motor_current_a   motor current, correlated with power
    running           1 while the machine is producing, 0 during downtime
    part_done         1 on the sample at which a part leaves the station
    part_ok           1 if that part passed, 0 if it was scrapped
    cycle_time_s      the cycle time of the part that just finished

Three realistic phenomena are injected, because each one gives a part of the
architecture something to do:

  * progressive wear  -- vibration and power drift upwards over the shift, so
    that the real machine diverges from its as-designed digital model.  This
    is the paper's own example for the Digital Models XC Functions: "a machine
    model assumes a given value of electric power / energy consumption, but
    the actual values will diverge as the real machine wears down" (S2.2).
  * a degradation event -- one station develops a vibration fault partway
    through the shift, which the Edge Analytics Engine must detect locally and
    the automation workflow must react to without a cloud round trip.
  * stochastic downtime and scrap -- so that availability, performance and
    quality are all strictly below 1 and OEE is non-trivial.

Ground truth is computed centrally over the raw samples and written next to
the data, so that the KPIs produced by the distributed pipeline (Edge
Analytics Engine -> Data Publishing Services -> global KPI) can be checked
against a single-process reference computation.

Usage:
    python dataset/generate_dataset.py                       # default shift
    python dataset/generate_dataset.py --minutes 10 --rate 10
    python dataset/generate_dataset.py --variant improved    # for what-if
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import random
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Tuple

HERE = os.path.dirname(os.path.abspath(__file__))

SIGNALS = ("vibration_mm_s", "temperature_c", "power_kw", "motor_current_a",
           "running", "part_done", "part_ok", "cycle_time_s")
COLUMNS = ("t", "station") + SIGNALS


# --------------------------------------------------------------------------
# station specification
# --------------------------------------------------------------------------
@dataclass
class StationSpec:
    """The *real* behaviour of a machine, which the digital model only
    approximates."""

    station: str
    true_cycle_time_s: float        # mean actual cycle time
    cycle_time_sigma_s: float       # cycle-to-cycle variation
    true_power_kw: float            # mean power draw while running
    power_sigma_kw: float
    base_vibration_mm_s: float
    vibration_sigma: float
    base_temperature_c: float
    scrap_rate: float               # probability a finished part is bad
    mtbf_s: float                   # mean time between (micro) stoppages
    mttr_s: float                   # mean time to repair a stoppage
    wear_rate_per_hour: float       # relative upward drift of vib. and power
    degradation_at_s: Optional[float] = None   # onset of a vibration fault
    degradation_gain: float = 2.2              # vibration multiplier at the end


def default_specs() -> List[StationSpec]:
    """A four-station line; ST3 is the bottleneck, ST2 develops a fault.

    The as-designed parameters used by the platform's digital models are
    (10.0, 11.0, 12.5, 9.5) s cycle time and (4.0, 6.5, 7.5, 3.5) kW.  The
    *true* values below deliberately differ, so model synchronisation has
    something real to correct.
    """
    return [
        StationSpec("ST1", 10.4, 0.45, 4.3, 0.30, 2.6, 0.30, 31.0,
                    0.012, 900.0, 25.0, 0.030),
        StationSpec("ST2", 11.6, 0.60, 7.1, 0.45, 3.1, 0.35, 33.5,
                    0.018, 720.0, 35.0, 0.045, degradation_at_s=1200.0),
        StationSpec("ST3", 13.4, 0.75, 8.2, 0.55, 3.4, 0.40, 35.0,
                    0.022, 600.0, 45.0, 0.055),
        StationSpec("ST4", 9.8, 0.40, 3.7, 0.25, 2.2, 0.25, 30.0,
                    0.009, 1100.0, 20.0, 0.025),
    ]


def improved_specs() -> List[StationSpec]:
    """The same line after the bottleneck station has been upgraded.

    Used to validate the Simulation Domain: the platform predicts the effect
    of shortening ST3's cycle time, and this variant provides the ground truth
    the prediction is compared against.
    """
    specs = default_specs()
    for spec in specs:
        if spec.station == "ST3":
            spec.true_cycle_time_s = 9.0
            spec.cycle_time_sigma_s = 0.5
    return specs


# --------------------------------------------------------------------------
# generator
# --------------------------------------------------------------------------
class ShiftGenerator:
    """Generates one production shift for a whole line."""

    def __init__(self, specs: List[StationSpec], duration_s: float,
                 sample_rate_hz: float, seed: int = 20180321):
        self.specs = specs
        self.duration_s = duration_s
        self.sample_rate_hz = sample_rate_hz
        self.seed = seed
        self.dt = 1.0 / sample_rate_hz
        self.n_samples = int(round(duration_s * sample_rate_hz))

    # -- one station -----------------------------------------------------
    def generate_station(self, spec: StationSpec) -> List[Dict[str, Any]]:
        rng = random.Random(f"{self.seed}:{spec.station}")
        rows: List[Dict[str, Any]] = []

        running = True
        downtime_left = 0.0
        time_into_cycle = 0.0
        target_cycle = max(0.5, rng.gauss(spec.true_cycle_time_s,
                                          spec.cycle_time_sigma_s))

        for i in range(self.n_samples):
            t = i * self.dt
            hours = t / 3600.0

            # --- availability: Poisson stoppages, exponential repair -----
            if running:
                if rng.random() < self.dt / spec.mtbf_s:
                    running = False
                    downtime_left = rng.expovariate(1.0 / spec.mttr_s)
            else:
                downtime_left -= self.dt
                if downtime_left <= 0:
                    running = True

            # --- wear and degradation ------------------------------------
            wear = 1.0 + spec.wear_rate_per_hour * hours
            degradation = 1.0
            if spec.degradation_at_s is not None and t >= spec.degradation_at_s:
                progress = min(1.0, (t - spec.degradation_at_s)
                               / max(1.0, self.duration_s - spec.degradation_at_s))
                degradation = 1.0 + (spec.degradation_gain - 1.0) * progress

            # --- signals ---------------------------------------------------
            if running:
                vibration = max(0.05, rng.gauss(
                    spec.base_vibration_mm_s * wear * degradation,
                    spec.vibration_sigma))
                power = max(0.05, rng.gauss(spec.true_power_kw * wear,
                                            spec.power_sigma_kw))
                # A small synchronous ripple over the cycle, as a real machine
                # has a load profile rather than a constant draw.
                power *= 1.0 + 0.08 * math.sin(2 * math.pi * time_into_cycle
                                               / max(0.5, target_cycle))
            else:
                vibration = max(0.02, rng.gauss(0.25, 0.05))
                power = max(0.02, rng.gauss(0.35, 0.05))   # idle draw

            temperature = rng.gauss(spec.base_temperature_c
                                    + 3.5 * hours + (1.5 if running else -0.5),
                                    0.35)
            current = power * 1.9 + rng.gauss(0.0, 0.08)

            # --- part completion -------------------------------------------
            part_done = 0
            part_ok = 1
            cycle_time = 0.0
            if running:
                time_into_cycle += self.dt
                if time_into_cycle >= target_cycle:
                    part_done = 1
                    cycle_time = time_into_cycle
                    scrap_risk = spec.scrap_rate * (1.6 if degradation > 1.3 else 1.0)
                    part_ok = 0 if rng.random() < scrap_risk else 1
                    time_into_cycle = 0.0
                    target_cycle = max(0.5, rng.gauss(spec.true_cycle_time_s,
                                                      spec.cycle_time_sigma_s))

            rows.append({
                "t": round(t, 4),
                "station": spec.station,
                "vibration_mm_s": round(vibration, 5),
                "temperature_c": round(temperature, 4),
                "power_kw": round(power, 5),
                "motor_current_a": round(current, 5),
                "running": 1 if running else 0,
                "part_done": part_done,
                "part_ok": part_ok,
                "cycle_time_s": round(cycle_time, 4),
            })
        return rows

    def generate(self) -> Dict[str, List[Dict[str, Any]]]:
        return {spec.station: self.generate_station(spec) for spec in self.specs}


# --------------------------------------------------------------------------
# centralised ground truth
# --------------------------------------------------------------------------
def ground_truth(rows_by_station: Dict[str, List[Dict[str, Any]]],
                 sample_rate_hz: float,
                 as_designed_cycle_times: Dict[str, float]) -> Dict[str, Any]:
    """Compute the KPIs centrally, in one process, over all raw samples.

    This is the reference the distributed pipeline is verified against.  The
    formulas are identical to those in `faredge.edge.analytics_engine`, but
    they are applied here to the complete raw dataset rather than
    incrementally and per gateway -- which is exactly the equivalence the
    paper's architecture has to deliver.
    """
    per_station: Dict[str, Dict[str, float]] = {}
    for station, rows in rows_by_station.items():
        n = len(rows)
        running = sum(r["running"] for r in rows)
        energy_kwh = sum(r["power_kw"] for r in rows) / sample_rate_hz / 3600.0
        parts_total = sum(r["part_done"] for r in rows)
        parts_good = sum(1 for r in rows if r["part_done"] and r["part_ok"])
        cycle_sum = sum(r["cycle_time_s"] for r in rows if r["part_done"])
        sq_vib = sum(r["vibration_mm_s"] ** 2 for r in rows)
        peak_vib = max((r["vibration_mm_s"] for r in rows), default=0.0)

        planned_time_s = n / sample_rate_hz
        run_time_s = running / sample_rate_hz
        ideal = as_designed_cycle_times.get(station, 0.0)

        availability = run_time_s / planned_time_s if planned_time_s else 0.0
        performance = (ideal * parts_total / run_time_s) if run_time_s else 0.0
        quality = parts_good / parts_total if parts_total else 0.0
        per_station[station] = {
            "samples": float(n),
            "planned_time_s": planned_time_s,
            "run_time_s": run_time_s,
            "parts_total": float(parts_total),
            "parts_good": float(parts_good),
            "availability": availability,
            "performance": performance,
            "quality": quality,
            "oee": availability * performance * quality,
            "throughput_pph": parts_good / (planned_time_s / 3600.0)
            if planned_time_s else 0.0,
            "mean_cycle_time_s": cycle_sum / parts_total if parts_total else 0.0,
            "energy_kwh": energy_kwh,
            "energy_per_part_kwh": energy_kwh / parts_good if parts_good else 0.0,
            "vibration_rms": math.sqrt(sq_vib / n) if n else 0.0,
            "peak_vibration_mm_s": peak_vib,
        }

    stations = list(per_station.values())
    total_planned = sum(s["planned_time_s"] for s in stations)
    total_parts = sum(s["parts_total"] for s in stations)
    sum_good = sum(s["parts_good"] for s in stations)
    sum_energy = sum(s["energy_kwh"] for s in stations)

    def weighted(key: str, weight_key: str) -> float:
        total_weight = sum(s[weight_key] for s in stations)
        if total_weight <= 0:
            return 0.0
        return sum(s[key] * s[weight_key] for s in stations) / total_weight

    global_kpis = {
        "plant_parts_good": sum_good,
        "plant_energy_kwh": sum_energy,
        "line_throughput_pph": min((s["throughput_pph"] for s in stations),
                                   default=0.0),
        "plant_availability": weighted("availability", "planned_time_s"),
        "plant_oee": weighted("oee", "planned_time_s"),
        "plant_mean_cycle_time_s": weighted("mean_cycle_time_s", "parts_total"),
        "plant_quality": sum_good / total_parts if total_parts else 0.0,
        "plant_energy_per_part_kwh": sum_energy / sum_good if sum_good else 0.0,
        "plant_peak_vibration_mm_s": max((s["peak_vibration_mm_s"]
                                          for s in stations), default=0.0),
    }
    return {"per_station": per_station, "global": global_kpis,
            "total_planned_time_s": total_planned}


# --------------------------------------------------------------------------
# persistence
# --------------------------------------------------------------------------
def write_csv(path: str, rows_by_station: Dict[str, List[Dict[str, Any]]]) -> int:
    """Write the whole line's data as one tidy CSV, ordered by time."""
    stations = sorted(rows_by_station)
    n = len(next(iter(rows_by_station.values()))) if rows_by_station else 0
    written = 0
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(COLUMNS))
        writer.writeheader()
        for i in range(n):
            for station in stations:
                writer.writerow(rows_by_station[station][i])
                written += 1
    return written


def read_csv(path: str) -> Dict[str, List[Dict[str, Any]]]:
    """Load a dataset back into the per-station form the platform consumes."""
    out: Dict[str, List[Dict[str, Any]]] = {}
    with open(path, newline="", encoding="utf-8") as handle:
        for raw in csv.DictReader(handle):
            row: Dict[str, Any] = {"station": raw["station"]}
            for key in ("t",) + SIGNALS:
                row[key] = float(raw[key])
            out.setdefault(raw["station"], []).append(row)
    return out


def generate_and_save(minutes: float = 30.0, rate_hz: float = 10.0,
                      variant: str = "default", seed: int = 20180321,
                      out_dir: str = HERE) -> Dict[str, Any]:
    specs = improved_specs() if variant == "improved" else default_specs()
    generator = ShiftGenerator(specs, minutes * 60.0, rate_hz, seed)
    rows_by_station = generator.generate()

    suffix = "" if variant == "default" else f"_{variant}"
    csv_path = os.path.join(out_dir, f"shopfloor{suffix}.csv")
    meta_path = os.path.join(out_dir, f"shopfloor{suffix}_metadata.json")
    truth_path = os.path.join(out_dir, f"shopfloor{suffix}_ground_truth.json")

    as_designed = {"ST1": 10.0, "ST2": 11.0, "ST3": 12.5, "ST4": 9.5}
    rows_written = write_csv(csv_path, rows_by_station)
    truth = ground_truth(rows_by_station, rate_hz, as_designed)

    metadata = {
        "source": "synthetic (the paper reports no dataset)",
        "paper": "Isaja & Soldatos, IFAC-PapersOnLine 51-11 (2018) 370-375",
        "variant": variant,
        "seed": seed,
        "sample_rate_hz": rate_hz,
        "duration_s": minutes * 60.0,
        "stations": [spec.station for spec in specs],
        "rows": rows_written,
        "columns": list(COLUMNS),
        "as_designed_models": {
            station: {"ideal_cycle_time_s": as_designed[station]}
            for station in as_designed if station in rows_by_station},
        "true_specs": [asdict(spec) for spec in specs],
        "phenomena": ["progressive wear", "vibration degradation on ST2",
                      "stochastic micro-stoppages", "scrap"],
    }
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(metadata, handle, indent=2)
    with open(truth_path, "w", encoding="utf-8") as handle:
        json.dump(truth, handle, indent=2)

    return {"csv": csv_path, "metadata": meta_path, "ground_truth": truth_path,
            "rows": rows_written, "stations": len(specs),
            "samples_per_station": generator.n_samples,
            "global_ground_truth": truth["global"]}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minutes", type=float, default=30.0,
                        help="shift length in minutes (default 30)")
    parser.add_argument("--rate", type=float, default=10.0,
                        help="sample rate in Hz (default 10)")
    parser.add_argument("--variant", choices=("default", "improved"),
                        default="default",
                        help="'improved' upgrades the bottleneck station")
    parser.add_argument("--seed", type=int, default=20180321)
    parser.add_argument("--both", action="store_true",
                        help="generate both variants")
    args = parser.parse_args()

    variants = ("default", "improved") if args.both else (args.variant,)
    for variant in variants:
        result = generate_and_save(args.minutes, args.rate, variant, args.seed)
        print(f"[{variant}] {result['rows']} rows "
              f"({result['samples_per_station']} samples x "
              f"{result['stations']} stations) -> {result['csv']}")
        for kpi, value in result["global_ground_truth"].items():
            print(f"    {kpi:<30} {value:,.6f}")


if __name__ == "__main__":
    main()
