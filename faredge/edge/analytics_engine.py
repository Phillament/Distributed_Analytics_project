"""Edge Analytics Engine Enabler (Edge Tier).

Paper, S3.2: "The Edge Analytics Engine Enabler implements the core part of
the Analytics Domain, as it defines a runtime environment for local data
analysis algorithms.  The environment includes both an execution engine --
i.e., a generic container for running data processing logic -- and
southbound/northbound interfaces for extracting raw Field data and sending
processed results to the Platform, respectively."

The structure follows that sentence exactly:

    AnalyticsAlgorithm      pluggable data-processing logic
    EdgeAnalyticsEngine     the generic container that runs it, with
      .southbound(record)     <- raw Field data in
      .northbound()           -> processed results out, to the Ledger Tier

The northbound path publishes to the Data Publishing Services Component, so
that "raw Field data is not transmitted on the network beyond the narrow
boundaries of a plant's LAN" (S3.3) -- only KPI deltas leave the gateway, and
only when they actually changed.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from ..common.messages import Event, StreamRecord

# Aggregation methods, mirrored from the Data Publishing contract.
SUM = "sum"
WEIGHTED_MEAN = "weighted_mean"
MIN = "min"
MAX = "max"
RATIO = "ratio"


@dataclass
class LocalResult:
    """One local analytics result, ready to be contributed to a global KPI."""

    kpi: str
    value: float = 0.0
    weight: float = 1.0
    numerator: Optional[float] = None
    denominator: Optional[float] = None
    method: str = WEIGHTED_MEAN
    unit: str = ""


class AnalyticsAlgorithm:
    """Base class for data processing logic deployed on the engine."""

    algorithm_id = "abstract"
    description = ""

    def reset(self) -> None:
        pass

    def consume(self, record: StreamRecord) -> None:
        raise NotImplementedError

    def results(self) -> List[LocalResult]:
        return []


# --------------------------------------------------------------------------
# Station KPI algorithm
# --------------------------------------------------------------------------
class StationKpiAlgorithm(AnalyticsAlgorithm):
    """Compute the standard OEE KPI set locally, on one station.

    This is the paper's own example of a distributed analytics process: "a
    standard set of Key Performance Indicators (KPI) may be measured locally on
    each EG" (S3.3).

    Definitions used (standard OEE decomposition):
        availability = run_time / planned_time
        performance  = ideal_cycle_time * parts_total / run_time
        quality      = parts_good / parts_total
        OEE          = availability * performance * quality
    """

    algorithm_id = "station_kpi"
    description = "Local OEE / throughput / energy KPIs for one station"

    def __init__(self, station: str, sample_rate_hz: float,
                 ideal_cycle_time_s: float):
        self.station = station
        self.sample_rate_hz = sample_rate_hz
        self.ideal_cycle_time_s = ideal_cycle_time_s
        self.reset()

    def reset(self) -> None:
        self.samples = 0
        self.running_samples = 0
        self.energy_kwh = 0.0
        self.sum_sq_vibration = 0.0
        self.vibration_samples = 0
        self.peak_vibration = 0.0
        self.parts_total = 0
        self.parts_good = 0
        self.cycle_time_sum = 0.0

    # -- southbound input -------------------------------------------------
    def consume(self, record: StreamRecord) -> None:
        values = record.values
        self.samples += 1
        dt_h = 1.0 / self.sample_rate_hz / 3600.0

        if values.get("running", 1.0) >= 0.5:
            self.running_samples += 1
        if "power_kw" in values:
            self.energy_kwh += values["power_kw"] * dt_h
        if "vibration_mm_s" in values:
            v = values["vibration_mm_s"]
            self.sum_sq_vibration += v * v
            self.vibration_samples += 1
            self.peak_vibration = max(self.peak_vibration, v)
        if values.get("part_done", 0.0) >= 0.5:
            self.parts_total += 1
            if values.get("part_ok", 1.0) >= 0.5:
                self.parts_good += 1
            self.cycle_time_sum += values.get("cycle_time_s", 0.0)

    # -- derived quantities ----------------------------------------------
    @property
    def planned_time_s(self) -> float:
        return self.samples / self.sample_rate_hz

    @property
    def run_time_s(self) -> float:
        return self.running_samples / self.sample_rate_hz

    @property
    def availability(self) -> float:
        return self.run_time_s / self.planned_time_s if self.planned_time_s else 0.0

    @property
    def performance(self) -> float:
        if self.run_time_s <= 0:
            return 0.0
        return self.ideal_cycle_time_s * self.parts_total / self.run_time_s

    @property
    def quality(self) -> float:
        return self.parts_good / self.parts_total if self.parts_total else 0.0

    @property
    def oee(self) -> float:
        return self.availability * self.performance * self.quality

    @property
    def throughput_pph(self) -> float:
        hours = self.planned_time_s / 3600.0
        return self.parts_good / hours if hours else 0.0

    @property
    def mean_cycle_time_s(self) -> float:
        return self.cycle_time_sum / self.parts_total if self.parts_total else 0.0

    @property
    def vibration_rms(self) -> float:
        return (math.sqrt(self.sum_sq_vibration / self.vibration_samples)
                if self.vibration_samples else 0.0)

    @property
    def energy_per_part_kwh(self) -> float:
        return self.energy_kwh / self.parts_good if self.parts_good else 0.0

    def local_kpis(self) -> Dict[str, float]:
        """The station-level view, for local use and for the Node dashboard."""
        return {
            "availability": self.availability,
            "performance": self.performance,
            "quality": self.quality,
            "oee": self.oee,
            "throughput_pph": self.throughput_pph,
            "mean_cycle_time_s": self.mean_cycle_time_s,
            "energy_kwh": self.energy_kwh,
            "energy_per_part_kwh": self.energy_per_part_kwh,
            "vibration_rms": self.vibration_rms,
            "parts_total": float(self.parts_total),
            "parts_good": float(self.parts_good),
        }

    # -- northbound contributions -----------------------------------------
    def results(self) -> List[LocalResult]:
        """Map local KPIs onto global KPIs and their aggregation rules."""
        planned = self.planned_time_s
        return [
            LocalResult("plant_parts_good", value=float(self.parts_good),
                        method=SUM, unit="parts"),
            LocalResult("plant_energy_kwh", value=self.energy_kwh,
                        method=SUM, unit="kWh"),
            LocalResult("line_throughput_pph", value=self.throughput_pph,
                        method=MIN, unit="parts/h"),
            LocalResult("plant_availability", value=self.availability,
                        weight=planned, method=WEIGHTED_MEAN, unit="ratio"),
            LocalResult("plant_oee", value=self.oee, weight=planned,
                        method=WEIGHTED_MEAN, unit="ratio"),
            LocalResult("plant_mean_cycle_time_s", value=self.mean_cycle_time_s,
                        weight=float(self.parts_total), method=WEIGHTED_MEAN,
                        unit="s"),
            LocalResult("plant_quality", numerator=float(self.parts_good),
                        denominator=float(self.parts_total), method=RATIO,
                        unit="ratio"),
            LocalResult("plant_energy_per_part_kwh", numerator=self.energy_kwh,
                        denominator=float(self.parts_good), method=RATIO,
                        unit="kWh/part"),
            LocalResult("plant_peak_vibration_mm_s", value=self.peak_vibration,
                        method=MAX, unit="mm/s"),
        ]


# --------------------------------------------------------------------------
# Anomaly detection algorithm
# --------------------------------------------------------------------------
class EwmaAnomalyDetector(AnalyticsAlgorithm):
    """Detect a degrading signal locally, close to the field.

    This algorithm is what makes "low-latency and proximity processing" (S1.2)
    measurable: the detection-to-reaction loop can be closed inside the Edge
    Gateway, instead of making a round trip to the Cloud.
    """

    algorithm_id = "ewma_anomaly"
    description = "EWMA + k-sigma detector on one signal"

    def __init__(self, signal: str, alpha: float = 0.02, k: float = 4.0,
                 warmup: int = 100,
                 on_anomaly: Optional[Callable[[Event], None]] = None):
        self.signal = signal
        self.alpha = alpha
        self.k = k
        self.warmup = warmup
        self.on_anomaly = on_anomaly
        self.reset()

    def reset(self) -> None:
        self.n = 0
        self.mean = 0.0
        self.var = 0.0
        self.anomalies = 0
        self.last_score = 0.0

    def consume(self, record: StreamRecord) -> None:
        value = record.values.get(self.signal)
        if value is None:
            return
        self.n += 1
        if self.n == 1:
            self.mean = value
            self.var = 0.0
            return
        deviation = value - self.mean
        self.mean += self.alpha * deviation
        self.var = (1 - self.alpha) * (self.var + self.alpha * deviation * deviation)
        sigma = math.sqrt(self.var) if self.var > 0 else 0.0
        self.last_score = abs(deviation) / sigma if sigma > 1e-9 else 0.0

        if self.n > self.warmup and sigma > 1e-9 and self.last_score > self.k:
            self.anomalies += 1
            if self.on_anomaly:
                self.on_anomaly(Event(
                    source=record.source, name="AnomalyDetected",
                    severity="WARN",
                    payload={"signal": self.signal, "value": value,
                             "mean": self.mean, "sigma": sigma,
                             "score": self.last_score}, t=record.t))

    def results(self) -> List[LocalResult]:
        return [LocalResult("plant_anomalies", value=float(self.anomalies),
                            method=SUM, unit="count")]


# --------------------------------------------------------------------------
# The Enabler
# --------------------------------------------------------------------------
class EdgeAnalyticsEngine:
    """Generic container for local data analysis algorithms."""

    def __init__(self, gateway_id: str, publish_epsilon: float = 0.005):
        self.gateway_id = gateway_id
        self.publish_epsilon = publish_epsilon
        self.algorithms: Dict[str, AnalyticsAlgorithm] = {}
        self._published: Dict[str, float] = {}
        self.records_consumed = 0
        self.publications = 0
        self.publications_suppressed = 0

    # -- deployment -------------------------------------------------------
    def deploy(self, algorithm: AnalyticsAlgorithm) -> AnalyticsAlgorithm:
        self.algorithms[algorithm.algorithm_id] = algorithm
        return algorithm

    def undeploy(self, algorithm_id: str) -> None:
        self.algorithms.pop(algorithm_id, None)

    # -- southbound interface: extract raw Field data ---------------------
    def southbound(self, record: StreamRecord) -> None:
        self.records_consumed += 1
        for algorithm in self.algorithms.values():
            algorithm.consume(record)

    # -- northbound interface: send processed results to the Platform -----
    def northbound(self, publisher: Callable[[LocalResult], bool],
                   force: bool = False) -> int:
        """Publish local results whose value has changed.

        "Each time a local KPI value changes, this change is submitted to the
        relevant Data Publishing service endpoint" (S3.3).  The dead band keeps
        the ledger in the regime it is good at -- frequent reads, infrequent
        writes.
        """
        published = 0
        for algorithm in self.algorithms.values():
            for result in algorithm.results():
                if not force and not self._changed(result):
                    self.publications_suppressed += 1
                    continue
                if publisher(result):
                    self._remember(result)
                    self.publications += 1
                    published += 1
        return published

    def _signature(self, result: LocalResult) -> float:
        if result.method == RATIO:
            den = result.denominator or 0.0
            return (result.numerator or 0.0) / den if den else 0.0
        return result.value

    def _changed(self, result: LocalResult) -> bool:
        current = self._signature(result)
        previous = self._published.get(result.kpi)
        if previous is None:
            return True
        scale = max(abs(previous), 1e-9)
        return abs(current - previous) / scale > self.publish_epsilon

    def _remember(self, result: LocalResult) -> None:
        self._published[result.kpi] = self._signature(result)

    # -- introspection ----------------------------------------------------
    def local_kpis(self) -> Dict[str, float]:
        out: Dict[str, float] = {}
        for algorithm in self.algorithms.values():
            if hasattr(algorithm, "local_kpis"):
                out.update(algorithm.local_kpis())
        return out

    def stats(self) -> Dict[str, Any]:
        return {"gateway": self.gateway_id,
                "algorithms": sorted(self.algorithms),
                "records_consumed": self.records_consumed,
                "publications": self.publications,
                "publications_suppressed": self.publications_suppressed}
