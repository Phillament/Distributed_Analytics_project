# FAR-EDGE: Distributed Ledger Architecture for Industrial Automation

A complete, working Python implementation of the reference architecture described in:

> **Mauro Isaja, John K. Soldatos**, *"Distributed Ledger Architecture for Automation, Analytics and
> Simulation in Industrial Environments"*, IFAC-PapersOnLine **51-11** (2018) 370–375.
> doi:[10.1016/j.ifacol.2018.08.321](https://doi.org/10.1016/j.ifacol.2018.08.321)

The paper defines the **FAR-EDGE Reference Architecture** (H2020 project 703094): a four-tier
platform for factory automation built on **edge computing** and a **distributed ledger with smart
contracts**. This repository turns that architecture into running code — all four Tiers, all 25
Platform Components, a hand-written blockchain with a PBFT-style consensus protocol, four Ledger
Services implemented as smart contracts, and a synthetic shop-floor dataset to drive it all.

Everything runs in one process with **no external dependencies** beyond `matplotlib`
(for the two output figures). The blockchain, the RSA signatures, the consensus protocol, the
MQTT/OPC-UA bindings and the analytics are all implemented from scratch.

---

## 1. What the paper specifies, and what is implemented

The paper describes the architecture from two viewpoints and then gives a per-Tier platform design.
Every element below exists as a concrete class in `faredge/`.

### Functional viewpoint (paper §2.2)

| | Element | Where it lives |
|---|---|---|
| **Functional Domain** | Automation | `edge/automation_services.py`, `ledger/services/orchestration.py`, `ledger/services/configuration.py` |
| **Functional Domain** | Analytics | `edge/analytics_engine.py`, `ledger/services/data_publishing.py` |
| **Functional Domain** | Simulation | `simulation/line_simulator.py` |
| **Crosscutting** | Management | `edge/node_management.py`, `cloud/internal_services.py` |
| **Crosscutting** | Security | `common/pdp.py`, `cloud/internal_services.py` |
| **Crosscutting** | Digital Models | `ledger/services/synchronization.py`, `cloud/internal_services.py` |
| **Crosscutting** | Field Abstraction & Data Routing | `edge/field_abstraction.py`, `edge/data_routing.py` |

### Structural viewpoint (paper §2.3) — four Tiers, two Scopes

```
                    ┌──────────────────────────────────────────────┐
  Enterprise Scope  │  CLOUD TIER — 11 components, 3 Open APIs     │
                    │  Open API Automation / Analytics /           │
                    │  Virtualization · IDM · Model Repository ·   │
                    │  Data Storage · Security Mgmt · PDP ·        │
                    │  Ledger Clients · Real-to-Digital Sync ·     │
                    │  Platform Management                         │
                    └──────────────────────┬───────────────────────┘
                                           │  WAN (~45 ms)
    ┌──────────────────────────────────────┴───────────────────────┐
    │  LEDGER TIER (logical — peers may sit in Edge or Cloud)      │
    │  Distributed Ledger Enabler on N Peer Nodes                  │
    │  Orchestration · Configuration · Data Publishing ·           │
    │  Synchronization   (four smart contracts)                    │
    └──────────────────────┬───────────────────────────────────────┘
                           │  peer-to-peer overlay (~4 ms)
    ┌──────────────────────┴───────────────────────────────────────┐
    │  EDGE TIER — one Edge Gateway per station, 7 components      │
    │  Edge Automation Services* · Edge Analytics Engine* ·        │
    │  Field Abstraction · Data Routing & Pre-processing ·         │
    │  PDP · Ledger Clients · Node Management      (* = Enabler)   │
    └──────────────────────┬───────────────────────────────────────┘
       Plant Scope         │  shop-floor LAN (~1 ms)
    ┌──────────────────────┴───────────────────────────────────────┐
    │  FIELD TIER — Edge Nodes, MQTT and OPC UA                    │
    │  Smart Objects (PLC: on-board logic, PDP, Ledger Clients)    │
    │  Connected Devices (sensors, actuators, RFID gate)           │
    │  wrapping Things · People · Environments                     │
    └──────────────────────────────────────────────────────────────┘
```

**25 Platform Components** in total: 2 (Field) + 7 (Edge) + 5 (Ledger) + 11 (Cloud), exactly as in
Figure 2 of the paper.

---

## 2. Quick start

```bash
git clone <your-repo-url> && cd far-edge-dlt
pip install -r requirements.txt
```

```bash
python dataset/generate_dataset.py --both
```

```bash
python run_demo.py
```

```bash
python -m unittest discover -s tests -v
```

`run_demo.py` boots the whole platform, runs a production shift against the dataset with one thread
per Edge Gateway, and prints six result blocks. It writes `results/demo_results.json` and two
figures to `results/figures/`.

Useful flags:

```bash
python run_demo.py --samples 3000 --stations 6 --peers 7 --protocol OPCUA
```

```bash
python run_demo.py --realtime
```

`--realtime` makes the network model actually sleep for the modelled link latencies, so the
reported timings are real wall-clock closed-loop latencies rather than pure throughput.

---

## 3. The dataset

The paper is an architecture paper: it reports no experiment and names no dataset. So this
repository ships a generator for a realistic one (`dataset/generate_dataset.py`).

A **four-station discrete manufacturing flow line**, each station driven by its own Edge Gateway,
sampled at 10 Hz:

| column | meaning |
|---|---|
| `t` | seconds since the start of the shift |
| `station` | `ST1` … `ST4` |
| `vibration_mm_s` | machine vibration velocity |
| `temperature_c` | bearing / ambient temperature |
| `power_kw` | instantaneous electrical power |
| `motor_current_a` | motor current |
| `running` | 1 while producing, 0 during a stoppage |
| `part_done` | 1 on the sample at which a part leaves the station |
| `part_ok` | 1 if that part passed inspection |
| `cycle_time_s` | cycle time of the part that just finished |

Three phenomena are injected on purpose, because each one gives part of the architecture something
to do:

* **progressive wear** — vibration and power drift upward, so the real machine diverges from its
  digital model. This is the paper's own motivating example for the Digital Models crosscutting
  function (§2.2);
* **a degradation event on ST2** — a vibration fault the Edge Analytics Engine must catch locally
  and the automation workflow must react to without a cloud round trip;
* **stochastic micro-stoppages and scrap** — so availability, performance and quality are all below
  1 and OEE is non-trivial.

Two variants are generated. `shopfloor.csv` is the baseline line; `shopfloor_improved.csv` is the
same line after the bottleneck station is upgraded, which gives the Simulation Domain's what-if
prediction something to be checked against.

Each variant ships with `*_ground_truth.json` — the same KPIs computed centrally, in one process,
over all raw samples. The demo compares the distributed result against it.

---

## 4. Results from a 10-minute shift

`python run_demo.py` on 4 stations × 6 000 samples (24 000 field samples), 5 Peer Nodes:

### Distributed analytics

Each Edge Gateway computes its station's KPIs locally; the Data Publishing smart contract
aggregates them into global KPIs by consensus.

| station | availability | performance | quality | OEE | parts good |
|---|---|---|---|---|---|
| ST1 | 0.985 | 0.947 | 1.000 | 0.933 | 56 |
| ST2 | 0.852 | 0.925 | 0.954 | 0.752 | 41 |
| ST3 | 1.000 | 0.917 | 0.977 | 0.896 | 43 |
| ST4 | 1.000 | 0.966 | 0.984 | 0.950 | 60 |

| global KPI | value | aggregation in the smart contract |
|---|---|---|
| `plant_parts_good` | 200 | `sum` |
| `plant_energy_kwh` | 3.721 | `sum` |
| `line_throughput_pph` | 246 | `min` (the bottleneck sets the line rate) |
| `plant_availability` | 0.959 | `weighted_mean` |
| `plant_oee` | 0.883 | `weighted_mean` |
| `plant_quality` | 0.980 | `ratio` (Σ good / Σ total) |
| `plant_peak_vibration_mm_s` | 4.974 | `max` |

**Largest deviation from a single-process calculation over the same raw data: 7.1 × 10⁻¹⁵** —
floating-point noise. The distributed pipeline and a centralised one give the same answers.

### Ledger activity

| metric | value |
|---|---|
| blocks committed | 267 |
| transactions committed | 568 |
| transactions rejected by a contract's business rules | 45 |
| transactions that failed consensus | 0 |
| mean commit latency | 37.1 ms |
| p95 commit latency | 57.8 ms |
| replicas holding an identical chain and state | 5 / 5 |

Every rejection came from a smart contract's business rules — a station claiming a part that was
not next in its route, or a parameter outside the safety envelope. None failed consensus.

### Distributed automation

No central orchestrator exists. Station locks, part routing and work-order state are all held on
the ledger.

| gateway | parts processed | claims refused by the routing rule | illegal reports blocked |
|---|---|---|---|
| EG-ST1 | 56 | 0 | 0 |
| EG-ST2 | 41 | 2 | 0 |
| EG-ST3 | 31 | 13 | 0 |
| EG-ST4 | 31 | 30 | 0 |

A refused claim is the contract enforcing the global routing rule: a station cannot take a part
that is not next in its route — ST4, at the end of the line, is refused most often while the line
fills up. Across 159 operations, `rule_violations = 0` and no two gateways ever held the same
station.

### Digital models and simulation

Gateways report observed behaviour; the Synchronization contract corrects a model only when the
relative drift exceeds 5 %.

| station | parameter | as designed | after sync | observed | drift |
|---|---|---|---|---|---|
| ST1 | `nominal_power_kw` | 4.00 | 4.12 | 4.25 | +6.2 % |
| ST2 | `ideal_cycle_time_s` | 11.00 | 11.35 | 11.70 | +6.3 % |
| ST3 | `nominal_power_kw` | 7.50 | 7.88 | 8.25 | +10.0 % |
| ST3 | `ideal_cycle_time_s` | 12.50 | 12.99 | 13.48 | +7.9 % |

6 corrections were applied by consensus and mirrored into the Cloud Model Repository.
The simulator — a pure consumer of those models — then predicts the line:

* bottleneck identified: **ST3** at 12.99 s
* predicted line throughput **260.6 parts/h** vs measured **246 parts/h** → **5.9 % error**
* what-if "shorten ST3 to 9.0 s": **+14.5 %** throughput, bottleneck moves to **ST2**

### Integrity, security and traffic

| check | result |
|---|---|
| all 5 replicas hold the identical chain and state | yes |
| every chain passes its hash-chain verification | yes |
| editing one committed record is detected | yes — *merkle root mismatch in block 1* |
| engineer calls Open API for Analytics | `200 ok` |
| guest calls the same endpoint | `403 no applicable policy (default deny)` |
| parameter outside the ledger-held safety envelope | rejected by consensus |

| link | messages | traffic | mean latency |
|---|---|---|---|
| shop-floor LAN (field ↔ edge) | 26 617 | 6.0 MiB | 1.16 ms |
| plant LAN (edge ↔ ledger) | 1 200 | 241.7 KiB | 2.32 ms |
| ledger peer-to-peer overlay | 3 484 | 2.5 MiB | 4.59 ms |
| WAN (plant → cloud) | 146 | 54.2 KiB | 50.0 ms |

**Raw field records that left the plant: 0.** 6.0 MiB of raw field data is processed inside the
plant, and 54 KiB of KPIs and filtered aggregates crosses the WAN.

> Numbers vary slightly between runs because the Edge Gateways run as real threads and interleave
> differently; the KPI equivalence and the integrity checks are deterministic.

---

## 5. Repository layout

```
far-edge-dlt/
├── run_demo.py                     one-command demo of the whole platform
├── requirements.txt
├── faredge/
│   ├── config.py                   Tiers, Scopes, Domains, all tunables
│   ├── platform.py                 wires the four Tiers together
│   ├── common/
│   │   ├── crypto.py               SHA-256, Merkle trees, RSA signatures
│   │   ├── messages.py             Event / Command / DataMessage / StreamRecord
│   │   ├── network.py              link model and byte accounting
│   │   ├── pdp.py                  Policy Decision Point (Security XC)
│   │   └── ledger_client.py        Ledger Clients, in three Tier flavours
│   ├── field/
│   │   ├── protocols.py            MQTT and OPC UA bindings
│   │   └── devices.py              Smart Objects, Connected Devices, entities
│   ├── edge/
│   │   ├── gateway.py              the Edge Gateway, hosting all 7 components
│   │   ├── field_abstraction.py    events / commands / data, protocol-agnostic
│   │   ├── data_routing.py         channels + dead band, decimation, windows
│   │   ├── analytics_engine.py     local KPI and anomaly algorithms
│   │   ├── automation_services.py  local workflow runtime
│   │   └── node_management.py      remote-management hooks
│   ├── ledger/
│   │   ├── blockchain.py           blocks, hash chain, world state
│   │   ├── smart_contract.py       contract base class + dynamic deployment
│   │   ├── consensus.py            three-phase majority / unanimity protocol
│   │   ├── peer_node.py            one replica + its contract engine
│   │   ├── dlt.py                  Distributed Ledger Enabler facade
│   │   └── services/               the four Ledger Services Components
│   ├── cloud/
│   │   ├── cloud_tier.py           the eleven Cloud-Tier components
│   │   ├── open_api.py             the three Open API Enablers
│   │   └── internal_services.py    IDM, Model Repository, Data Storage, …
│   └── simulation/line_simulator.py
├── dataset/generate_dataset.py     synthetic shop-floor data + ground truth
├── tests/test_platform.py          18 unit and end-to-end tests
├── docs/CODE_EXPLANATION.md        step-by-step walkthrough of the code
└── results/                        JSON results and figures
```

---

## 6. Design notes

**Consensus.** The paper states two thresholds, and both are implemented literally:
a *majority* of Peer Nodes to append a record, and *system-wide unanimity* to amend or remove one.
The protocol is three-phase: the round's proposer executes the batch and broadcasts a signed
candidate block; every online peer **independently re-executes** it against its own replica and
votes on whether the resulting state root matches; on quorum, every honest peer commits. Because
validation is independent re-execution, a Byzantine proposer cannot forge a state root that honest
peers will reproduce. With 5 peers the system tolerates 2 crashed peers and outvotes a Byzantine
one — both covered by tests.

The paper says "a majority consensus of PNs" without saying majority of *which* set, so
`LedgerConfig.quorum_basis` selects between `"known"` (majority of all peers — the standard quorum
rule, the default) and `"online"` (majority of those currently reachable).

**Determinism.** Smart contracts are pure functions of `(state, call)`. They may not read the wall
clock or any source of randomness — the caller's timestamp is part of the signed transaction — and
dynamically deployed contracts are compiled in a restricted namespace with no `__import__`.
Determinism is what lets every peer execute the same call in parallel and compare state roots.

**Cryptography.** SHA-256 hash chaining, Merkle roots over block transactions, and textbook RSA
signatures implemented on the standard library, so the project has no crypto dependency. Keys
default to a 512-bit modulus: genuinely asymmetric, fast enough to spin up dozens of nodes in a
unit test. **This is simulation grade.** Replace it with a vetted library (ECDSA P-256) before
anything real.

**Threading.** Peer Nodes and Edge Gateways are real threads communicating over links that model
latency and count bytes, so the consensus protocol is genuinely exercised under concurrency. Set
`network.real_sleep = False` for fast runs where only byte accounting matters.

**What is simulated rather than deployed.** Network links are modelled, not sockets; the field
protocols are faithful in addressing, framing overhead and subscription semantics but are not real
MQTT or OPC UA stacks; the Cloud Tier's Open APIs are Python call interfaces rather than HTTPS
endpoints. The architecture, the component boundaries, the consensus protocol and the data flow
are real.

---

## 7. Documentation

* **[docs/CODE_EXPLANATION.md](docs/CODE_EXPLANATION.md)** — a step-by-step walkthrough of every
  module, what it does and which part of the paper it realises.
* **[presentation/](presentation/)** — slide deck covering the paper, the methodology, the
  implementation and the results.

## 8. Requirements

Python 3.10+ and `matplotlib` (figures only — the platform itself is pure standard library).

## 9. License

MIT. The paper itself is © 2018 IFAC, published by Elsevier; this repository is an independent
implementation of the architecture it describes, not a redistribution of the paper.
