"""FAR-EDGE demo: run the whole four-tier platform on the shop-floor dataset.

This single script exercises the complete methodology of the paper:

    Field Tier   -> Smart Objects and Connected Devices publish field data
    Edge Tier    -> Edge Gateways pre-process it, compute local KPIs and run
                    local automation workflows
    Ledger Tier  -> Peer Nodes agree, by consensus, on the global state:
                    part routing, configuration, global KPIs, digital models
    Cloud Tier   -> the three Open APIs expose everything to the outside world

It prints six short result blocks and writes them to results/demo_results.json
plus two figures in results/figures/.

Usage:
    python run_demo.py                 # 10 minutes of shift data per station
    python run_demo.py --samples 3000  # shorter run
    python run_demo.py --stations 6    # bigger line
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import time
from typing import Any, Dict, List

from dataset.generate_dataset import generate_and_save, ground_truth, read_csv
from faredge.common.network import (FIELD_LAN, MONITOR, PEER_P2P, PLANT_LAN,
                                    WAN_CLOUD)
from faredge.config import PlatformConfig
from faredge.platform import FarEdgePlatform, default_station_profiles

ROOT = os.path.dirname(os.path.abspath(__file__))
RESULTS = os.path.join(ROOT, "results")
FIGURES = os.path.join(RESULTS, "figures")
DATASET_CSV = os.path.join(ROOT, "dataset", "shopfloor.csv")


# --------------------------------------------------------------------------
# small console helpers
# --------------------------------------------------------------------------
def banner(title: str) -> None:
    print()
    print("=" * 76)
    print(title)
    print("=" * 76)


def fmt(value: Any) -> str:
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        if value and abs(value) < 1e-4:
            return f"{value:.2e}"
        return f"{value:,.4f}"
    if isinstance(value, int):
        return f"{value:,}"
    return str(value)


def table(headers: List[str], rows: List[List[Any]]) -> str:
    cells = [[fmt(v) for v in row] for row in rows]
    widths = [max(len(headers[i]), *(len(r[i]) for r in cells)) if cells
              else len(headers[i]) for i in range(len(headers))]
    out = ["  ".join(h.ljust(w) for h, w in zip(headers, widths)),
           "  ".join("-" * w for w in widths)]
    out += ["  ".join(c.ljust(w) for c, w in zip(row, widths)) for row in cells]
    return "\n".join(out)


def human_bytes(n: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB"):
        if abs(n) < 1024.0:
            return f"{n:,.1f} {unit}"
        n /= 1024.0
    return f"{n:,.1f} TiB"


# --------------------------------------------------------------------------
# the demo
# --------------------------------------------------------------------------
def main() -> Dict[str, Any]:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", type=int, default=6000,
                        help="field samples per station (default 6000 = 10 min)")
    parser.add_argument("--stations", type=int, default=4)
    parser.add_argument("--peers", type=int, default=5,
                        help="number of Peer Nodes in the Ledger Tier")
    parser.add_argument("--protocol", choices=("MQTT", "OPCUA"), default="MQTT")
    parser.add_argument("--realtime", action="store_true",
                        help="actually sleep for modelled network latency")
    args = parser.parse_args()

    os.makedirs(FIGURES, exist_ok=True)
    results: Dict[str, Any] = {"run": vars(args)}

    # ------------------------------------------------------------------
    # 0. dataset
    # ------------------------------------------------------------------
    banner("0. DATASET")
    if not os.path.exists(DATASET_CSV):
        print("generating the shop-floor dataset ...")
        generate_and_save()
    rows = read_csv(DATASET_CSV)
    rows = {station: series[:args.samples] for station, series in rows.items()
            if station in [p.station for p in default_station_profiles(args.stations)]}
    sample_rate = 10.0
    print(f"{len(rows)} stations x {args.samples:,} samples "
          f"= {sum(len(v) for v in rows.values()):,} field samples "
          f"({args.samples / sample_rate / 60:.1f} min of production)")

    truth = ground_truth(rows, sample_rate,
                         {p.station: p.ideal_cycle_time_s
                          for p in default_station_profiles(args.stations)})
    results["dataset"] = {"stations": sorted(rows),
                          "samples_per_station": args.samples,
                          "sample_rate_hz": sample_rate}

    # ------------------------------------------------------------------
    # 1. build and bootstrap the platform
    # ------------------------------------------------------------------
    banner("1. PLATFORM: four tiers, 25 components")
    cfg = PlatformConfig(n_stations=args.stations)
    cfg.ledger.n_peer_nodes = args.peers
    cfg.field_tier.protocol = args.protocol
    cfg.network.real_sleep = args.realtime
    MONITOR.reset()

    platform = FarEdgePlatform(cfg)
    started = time.perf_counter()
    platform.bootstrap("WO-DEMO", qty=100_000)
    bootstrap_s = time.perf_counter() - started

    inventory = platform.tier_inventory()
    print(table(["tier", "scope", "contents"], [
        ["FIELD", inventory["FIELD"]["scope"],
         f"{inventory['FIELD']['smart_objects']} Smart Objects + "
         f"{inventory['FIELD']['connected_devices']} Connected Devices, "
         f"{args.protocol}"],
        ["EDGE", inventory["EDGE"]["scope"],
         f"{inventory['EDGE']['gateways']} Edge Gateways x 7 components"],
        ["LEDGER", inventory["LEDGER"]["scope"],
         f"{inventory['LEDGER']['peer_nodes']} Peer Nodes, "
         f"{len(inventory['LEDGER']['services'])} Ledger Services"],
        ["CLOUD", inventory["CLOUD"]["scope"],
         f"{len(inventory['CLOUD']['components'])} components, 3 Open APIs"],
    ]))
    print(f"\nLedger Services deployed: {', '.join(inventory['LEDGER']['services'])}")
    print(f"Consensus: majority = {platform.ledger.quorum_for()} of "
          f"{args.peers} Peer Nodes for an append; unanimity to amend")
    print(f"bootstrap took {bootstrap_s:.2f} s")
    results["platform"] = {"bootstrap_s": round(bootstrap_s, 3),
                           "tiers": inventory}

    # ------------------------------------------------------------------
    # 2. run the shift
    # ------------------------------------------------------------------
    banner("2. PRODUCTION RUN (one thread per Edge Gateway)")
    report = platform.run_shift(rows)
    print(f"{report.samples_processed:,} field samples processed in "
          f"{report.wall_time_s:.2f} s "
          f"({report.samples_processed / max(report.wall_time_s, 1e-9):,.0f} samples/s)")
    ledger_stats = report.ledger
    print()
    print(table(["ledger metric", "value"],
                [[k.replace("_", " "), v] for k, v in ledger_stats.items()]))
    results["production_run"] = report.as_dict()

    # ------------------------------------------------------------------
    # 3. distributed analytics: local KPIs -> global KPIs by consensus
    # ------------------------------------------------------------------
    banner("3. ANALYTICS: local KPIs on each gateway, global KPIs on the ledger")
    station_kpis = platform.station_kpis()
    print(table(["station", "availability", "performance", "quality", "OEE",
                 "parts good", "kWh"],
                [[st, k["availability"], k["performance"], k["quality"],
                  k["oee"], int(k["parts_good"]), k["energy_kwh"]]
                 for st, k in sorted(station_kpis.items())]))

    global_kpis = platform.global_kpis()
    checked = []
    for kpi, entry in sorted(global_kpis.items()):
        reference = truth["global"].get(kpi)
        checked.append([kpi, entry["value"], entry["method"],
                        entry["contributors"],
                        "-" if reference is None else abs(entry["value"] - reference)])
    print("\nGlobal KPIs, aggregated inside the Data Publishing smart contract:")
    print(table(["global KPI", "value", "aggregation", "contributing EGs",
                 "vs central calc"], checked))
    errors = [row[4] for row in checked if isinstance(row[4], float)]
    print(f"\nLargest difference against a single-process calculation over the "
          f"same raw data: {max(errors):.2e}")
    results["analytics"] = {"station_kpis": station_kpis,
                            "global_kpis": global_kpis,
                            "max_difference_vs_central": max(errors)}

    # ------------------------------------------------------------------
    # 4. distributed automation: the ledger enforced the rules
    # ------------------------------------------------------------------
    banner("4. AUTOMATION: distributed workflows coordinated by smart contracts")
    counters = platform.cloud.ledger_clients.orchestration.counters()
    locks = platform.cloud.ledger_clients.read("OrchestrationServices", "locks", {})
    print(table(["orchestration counter", "value"],
                [[k.replace("_", " "), v] for k, v in sorted(counters.items())]))
    print(f"\nStation locks held (distributed mutual exclusion): "
          f"{ {s: v['eg'] for s, v in locks.items()} }")

    workflow_rows = []
    for gid, gateway in sorted(platform.gateways.items()):
        stats = {w["workflow"]: w for w in gateway.automation.stats()["workflows"]}
        cycle = stats.get("station_cycle", {})
        safety = stats.get("safety_reaction", {})
        workflow_rows.append([gid, cycle.get("parts_processed", 0),
                              cycle.get("claims_refused", 0),
                              cycle.get("reports_refused", 0),
                              safety.get("reactions", 0),
                              safety.get("mean_reaction_ms", 0.0),
                              gateway.plc.local_trips])
    print()
    print(table(["gateway", "parts processed", "claims refused by rule",
                 "illegal reports blocked", "local safety reactions",
                 "mean reaction ms", "on-board trips"], workflow_rows))
    print("\nA refused claim is the ledger enforcing the global routing rule: a "
          "station\ncannot take a part that is not next in its route.")
    results["automation"] = {"counters": counters,
                             "locks": {s: v["eg"] for s, v in locks.items()},
                             "workflows": workflow_rows}

    # ------------------------------------------------------------------
    # 5. digital models and simulation
    # ------------------------------------------------------------------
    banner("5. DIGITAL MODELS: drift detection, correction and simulation")
    models = platform.cloud.ledger_clients.synchronization.models()
    model_rows = []
    for station in sorted(models):
        model = models[station]
        designed = platform.as_designed_models[station]
        for param, value in sorted(model["params"].items()):
            drift = model.get("drift", {}).get(param, {})
            model_rows.append([station, param, designed.get(param, 0.0), value,
                               drift.get("observed", 0.0),
                               drift.get("relative", 0.0), model["version"]])
    print(table(["station", "parameter", "as designed", "model now", "observed",
                 "rel. drift", "model v"], model_rows))
    print(f"\nModel corrections applied by consensus: "
          f"{platform.cloud.ledger_clients.read('SynchronizationServices', 'correction_count', 0)}"
          f"   |   mirrored into the Cloud Model Repository: "
          f"{platform.cloud.real_to_digital.updates_applied}")

    duration_s = args.samples / sample_rate
    token = platform.cloud.login("line_engineer")
    sim_synced = platform.cloud.open_api_virtualization.call(
        "runSimulation", token=token, duration_s=duration_s,
        use_synced_models=True)
    what_if = platform.cloud.open_api_virtualization.call(
        "runWhatIf", token=token, duration_s=duration_s,
        overrides={"ST3": {"ideal_cycle_time_s": 9.0}})
    measured = truth["global"]["line_throughput_pph"]
    predicted = sim_synced.data["predicted_throughput_pph"]
    print()
    print(table(["simulation (Open API for Virtualization)", "value"], [
        ["bottleneck station", sim_synced.data["bottleneck_station"]],
        ["bottleneck cycle time (s)", sim_synced.data["bottleneck_cycle_time_s"]],
        ["predicted line throughput (parts/h)", predicted],
        ["measured line throughput (parts/h)", measured],
        ["prediction error (%)", abs(predicted - measured) / measured * 100.0],
        ["what-if: ST3 cycle time -> 9.0 s", ""],
        ["  predicted throughput change (%)", what_if.data["throughput_change_pct"]],
        ["  bottleneck moves to", what_if.data["new_bottleneck"]],
    ]))
    results["simulation"] = {"models": models, "run": sim_synced.data,
                             "what_if": what_if.data,
                             "measured_throughput_pph": measured}

    # ------------------------------------------------------------------
    # 6. ledger integrity, security and traffic
    # ------------------------------------------------------------------
    banner("6. LEDGER INTEGRITY, SECURITY AND NETWORK TRAFFIC")
    replicas = platform.ledger.verify_replicas()
    print(table(["ledger check", "result"], [
        ["Peer Nodes", len(platform.ledger.peers)],
        ["chain height", platform.ledger.chain_height()],
        ["all replicas hold the identical chain", replicas["chains_identical"]],
        ["all replicas hold the identical state", replicas["states_identical"]],
        ["every chain passes its hash-chain check", replicas["all_chains_intact"]],
        ["ledger size on disk-equivalent", human_bytes(platform.ledger.ledger_size_bytes())],
    ]))

    # tamper test: modify one committed record and re-verify
    victim = platform.ledger.peers[0]
    target = victim.chain.blocks[1]
    saved = json.loads(json.dumps(target.calls))
    target.calls[0]["call"]["args"]["tampered"] = True
    tampered_ok, tampered_reason = victim.chain.verify_integrity()
    target.calls = saved
    restored_ok, _ = victim.chain.verify_integrity()
    print(f"\nTamper test: editing one committed record makes the chain check "
          f"fail -> {'detected' if not tampered_ok else 'NOT detected'}"
          f" ({tampered_reason}); after restoring: "
          f"{'intact' if restored_ok else 'still broken'}")

    # security: the Policy Decision Point in action
    denied = platform.cloud.open_api_analytics.call(
        "getGlobalKPIs", token=platform.cloud.login("guest"))
    allowed = platform.cloud.open_api_analytics.call("getGlobalKPIs", token=token)
    out_of_envelope = platform.cloud.ledger_clients.configuration.set_parameter(
        platform.stations[0], "speed_factor", 5.0, reason="demo: outside limits")
    print()
    print(table(["security check", "outcome"], [
        ["engineer calls Open API for Analytics",
         f"{allowed.status} {'ok' if allowed.ok else allowed.reason}"],
        ["guest calls the same endpoint",
         f"{denied.status} {denied.reason}"],
        ["parameter outside the ledger-held safety envelope",
         f"rejected by consensus: {out_of_envelope.reason}"
         if not out_of_envelope.accepted else "ACCEPTED (unexpected)"],
    ]))

    net = MONITOR.summary()
    print()
    print(table(["link", "messages", "traffic", "mean latency (ms)"],
                [[link, net[link]["messages"], human_bytes(net[link]["bytes"]),
                  net[link]["mean_latency_ms"]]
                 for link in (FIELD_LAN, PLANT_LAN, PEER_P2P, WAN_CLOUD)
                 if link in net]))
    raw_out = sum(MONITOR.egress_payload_kinds.get(k, 0)
                  for k in ("RAW_STREAM", "DATA", "STREAM"))
    print(f"\nRaw field records that left the plant: {raw_out} "
          f"(the gateways publish KPIs and filtered aggregates instead)")

    results["ledger_integrity"] = {
        "replicas": replicas,
        "tamper_detected": not tampered_ok,
        "tamper_reason": tampered_reason,
        "chain_restored": restored_ok,
    }
    results["security"] = {
        "engineer_status": allowed.status, "guest_status": denied.status,
        "guest_reason": denied.reason,
        "envelope_violation_rejected": not out_of_envelope.accepted,
        "envelope_violation_reason": out_of_envelope.reason,
        "pdp": {gid: eg.pdp.stats.as_dict()
                for gid, eg in platform.gateways.items()},
    }
    results["network"] = net
    results["raw_records_leaving_plant"] = raw_out

    platform.shutdown()

    # ------------------------------------------------------------------
    # figures and json
    # ------------------------------------------------------------------
    figures = make_figures(station_kpis, global_kpis, net, model_rows)
    path = os.path.join(RESULTS, "demo_results.json")
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(results, handle, indent=2, default=str)

    banner("OUTPUT")
    print(f"results  -> {os.path.relpath(path, ROOT)}")
    for figure in figures:
        print(f"figure   -> {os.path.relpath(figure, ROOT)}")
    return results


# --------------------------------------------------------------------------
# figures
# --------------------------------------------------------------------------
def make_figures(station_kpis, global_kpis, net, model_rows) -> List[str]:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np
    except Exception as exc:                      # pragma: no cover
        print(f"(figures skipped: {exc})")
        return []

    saved: List[str] = []

    # Figure 1: station KPIs and the global KPI they roll up into.
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.2))
    stations = sorted(station_kpis)
    x = np.arange(len(stations))
    for offset, key, colour in ((-0.25, "availability", "#2b6cb0"),
                                (0.0, "quality", "#2f855a"),
                                (0.25, "oee", "#c05621")):
        axes[0].bar(x + offset, [station_kpis[s][key] for s in stations], 0.25,
                    label=key, color=colour)
    plant_oee = global_kpis.get("plant_oee", {}).get("value")
    if plant_oee is not None:
        axes[0].axhline(plant_oee, ls="--", c="#4a5568",
                        label=f"plant OEE (ledger) = {plant_oee:.3f}")
    axes[0].set_xticks(x)
    axes[0].set_xticklabels(stations)
    axes[0].set_ylim(0, 1.30)
    axes[0].set_ylabel("ratio")
    axes[0].set_title("Local KPIs per Edge Gateway")
    axes[0].legend(fontsize=8, ncol=2, loc="upper center")
    axes[0].grid(axis="y", alpha=0.3)

    axes[1].bar(x, [station_kpis[s]["throughput_pph"] for s in stations],
                0.5, color="#6b46c1")
    line = global_kpis.get("line_throughput_pph", {}).get("value")
    if line is not None:
        axes[1].axhline(line, ls="--", c="#4a5568",
                        label=f"line throughput (min) = {line:.0f} parts/h")
        axes[1].legend(fontsize=8)
    axes[1].set_xticks(x)
    axes[1].set_xticklabels(stations)
    axes[1].set_ylabel("parts / h")
    axes[1].set_title("Station throughput and the global aggregate")
    axes[1].grid(axis="y", alpha=0.3)
    fig.suptitle("Analytics Domain: local computation, global aggregation by consensus")
    path = os.path.join(FIGURES, "kpis.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    saved.append(path)

    # Figure 2: where the traffic goes, and model drift.
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.2))
    links = [l for l in (FIELD_LAN, PLANT_LAN, PEER_P2P, WAN_CLOUD) if l in net]
    labels = {FIELD_LAN: "field LAN\n(field<->edge)", PLANT_LAN: "plant LAN\n(edge<->ledger)",
              PEER_P2P: "ledger p2p\n(peer<->peer)", WAN_CLOUD: "WAN\n(plant->cloud)"}
    axes[0].bar([labels[l] for l in links],
                [net[l]["bytes"] / 1024.0 for l in links],
                color=["#2b6cb0", "#2f855a", "#6b46c1", "#c05621"])
    axes[0].set_yscale("log")
    axes[0].set_ylabel("traffic (KiB, log scale)")
    axes[0].set_title("Traffic stays inside the plant")
    axes[0].tick_params(axis="x", labelsize=9)
    axes[0].grid(axis="y", alpha=0.3)

    drift_rows = [r for r in model_rows if r[5]]
    if drift_rows:
        names = [f"{r[0]}\n{'cycle time' if 'cycle' in r[1] else 'power'}"
                 for r in drift_rows]
        axes[1].bar(names, [r[5] * 100.0 for r in drift_rows], color="#319795")
        axes[1].axhline(5.0, ls="--", c="#c53030", label="correction threshold 5 %")
        axes[1].axhline(-5.0, ls="--", c="#c53030")
        axes[1].set_ylabel("relative drift (%)")
        axes[1].set_title("Digital Models XC: real vs modelled")
        axes[1].legend(fontsize=8)
        axes[1].tick_params(axis="x", labelsize=8)
        axes[1].grid(axis="y", alpha=0.3)
    fig.suptitle("Edge filtering and model synchronisation")
    path = os.path.join(FIGURES, "traffic_and_drift.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    saved.append(path)
    return saved


if __name__ == "__main__":
    main()
