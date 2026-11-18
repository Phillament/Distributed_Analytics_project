# Code explanation — module by module

This document walks through the implementation in the order the data actually flows: a sensor
reading is produced in the Field Tier, pre-processed and analysed in the Edge Tier, agreed upon in
the Ledger Tier, and exposed in the Cloud Tier. For each module it says **what it does**, **how it
works**, and **which part of the paper it realises**.

Paper: Isaja & Soldatos, *"Distributed Ledger Architecture for Automation, Analytics and Simulation
in Industrial Environments"*, IFAC-PapersOnLine 51-11 (2018) 370–375.

---

## 0. The shape of the whole thing

```
dataset row  ─▶  Field Tier  ─▶  Edge Tier  ─▶  Ledger Tier  ─▶  Cloud Tier
 (one sample)    publish        filter +        consensus        Open APIs
                 over MQTT      local KPIs      on global        + simulation
                                + local         state
                                automation
```

`faredge/platform.py` builds all of it from one `PlatformConfig` and is the only place that knows
about more than one Tier. Everything else talks to its neighbours through narrow interfaces.

---

## 1. `faredge/config.py` — the vocabulary of the architecture

Before any behaviour, the paper's structural concepts are made into named constants so that the
rest of the code can refer to them explicitly rather than by convention:

```python
class Tier:   FIELD, EDGE, LEDGER, CLOUD        # paper §2.3
class Scope:  PLANT, ENTERPRISE                 # paper §2.3
class Domain: AUTOMATION, ANALYTICS, SIMULATION # paper §2.2
class XC:     MANAGEMENT, SECURITY, DIGITAL_MODELS, FIELD_ABSTRACTION_DATA_ROUTING
```

Then four dataclasses hold every tunable: `NetworkConfig` (link latencies), `LedgerConfig`
(peer count, quorum rule, block batching, key size, fault injection), `EdgeConfig` (dead band,
analytics window, KPI publish threshold) and `FieldConfig` (sample rate, protocol).

The link latencies encode the geography the paper argues about — 1 ms from field to edge, 4 ms
across the ledger overlay, 45 ms out to the cloud. `real_sleep` decides whether a link actually
blocks for that time (realistic timing) or only accounts for it (fast runs).

---

## 2. `faredge/common/` — things every Tier needs

### `crypto.py` — making the ledger tamper-evident

The paper requires that ledger state be *"secured by cryptographic techniques, so that records in
the ledger cannot be modified or removed once created"* (§3.3). Three primitives deliver that:

* `sha256_hex` and `canonical_json` — deterministic serialisation, so that two peers hashing the
  same object always get the same digest. Without this, consensus on a state root is impossible.
* `merkle_root(leaves)` — a Merkle tree over a block's transaction hashes. Changing any
  transaction changes the root, which is part of the block header, which is part of the block
  hash, which the next block commits to.
* `Identity` / `verify` — textbook RSA on the standard library. `Identity.sign(payload)` hashes
  the payload and raises it to the private exponent; `verify` does the inverse. A `KeyDirectory`
  maps addresses to public keys so a peer can check who authored a transaction.

RSA key generation uses Miller–Rabin to find primes. Keys default to a 512-bit modulus: genuinely
asymmetric, fast enough that spinning up a dozen nodes in a test is instant. This is explicitly
simulation grade and the docstring says so.

### `messages.py` — the four message types

The paper is specific about what the Field Abstraction Component carries: *"It only deals with
events, commands and data messages … it does not deal with data streams or any kind of massive
data flow, as such concerns belong to Data Routing"* (§3.2). So there are exactly four types:

| type | direction | handled by |
|---|---|---|
| `DataMessage` | northbound | Field Abstraction |
| `Event` | northbound | Field Abstraction |
| `Command` | southbound | Field Abstraction |
| `StreamRecord` | northbound, high volume | **Data Routing only** |

Every message mixes in `_Wire`, which gives it a `wire_size()` computed from its canonical JSON.
That one method is what makes all the traffic accounting possible.

`ServiceCall` is a call on a Ledger Service endpoint. Its `digest()` is the exact subset that gets
signed, hashed and executed — including the caller's timestamp, so that a contract never has to
read a clock.

### `network.py` — links that cost something

A `Link` has a base latency and jitter. `transmit(bytes, kind)` draws a latency, optionally sleeps
for it, and records the bytes against one of four link classes: `field_lan`, `plant_lan`,
`peer_p2p`, `wan_cloud`. A module-level `MONITOR` accumulates everything, including a tally of
which payload kinds ever crossed the plant boundary — that tally is how the demo shows that zero
raw field records leave the shop floor.

### `pdp.py` — the Security crosscutting function

The paper puts a Policy Decision Point in the Field, Edge and Cloud Tiers, and notes that the Edge
one *"is not constrained by low-end computing resources, as is the case with the Field Tier
version"* (§3.2). One class serves all three; what differs is the decision-cache capacity:

```python
CACHE_CAPACITY = {"FIELD": 2, "EDGE": 64, "CLOUD": 1024}
```

A Field-Tier PDP therefore misses its cache more often and has to fetch policy across the network
from the central Policy Administration Point — the trade-off the paper is pointing at, made
measurable.

`evaluate(subject, action, resource)` combines matching policies with **deny-overrides** and
**default-deny**. Policies are versioned; bumping the version at the administration point
invalidates every cached decision everywhere.

### `ledger_client.py` — the Ledger Clients Component, three times

The paper describes this Component in three Tiers with a deliberate capability ladder, and the
class hierarchy mirrors it exactly:

```python
FieldLedgerClients   # outgoing calls only            (§3.1)
  └─ EdgeLedgerClients   # + subscribe to ledger events  (§3.2)
       └─ CloudLedgerClients  # same, over the WAN       (§3.4)
```

Inside each, *"a dedicated client for each Ledger Service"*, as the paper says:
`.orchestration`, `.configuration`, `.data_publishing`, `.synchronization`. Each sub-client turns
a method call into a signed `ServiceCall`, charges it to the owning Tier's link, and returns a
`Receipt` saying whether consensus accepted it and why not if it did not.

Reads take a separate, cheap path: no consensus, served by one replica.

---

## 3. `faredge/field/` — the Field Tier

### `protocols.py` — MQTT and OPC UA

The paper states that the implementation *"supports the OPC UA and the MQTT field communication
protocols"* (§3.2). Both are modelled behind one adapter interface, and they differ where real
protocols differ:

| | MQTT | OPC UA |
|---|---|---|
| addressing | `plant/line1/ST1-SENS/vibration_mm_s` | `ns=2;s=ST1-SENS.vibration_mm_s` |
| framing overhead | 28 B | 76 B |
| subscription | `+` and `#` wildcards | monitored items, `Device.*` |

Switching a station from one to the other changes nothing above the adapter — which is the entire
point of the Field Abstraction Component, and is checked by the demo's `--protocol` flag.

### `devices.py` — Edge Nodes and the real world

The paper distinguishes **Smart Objects** (on-board compute) from **Connected Devices** (none), and
says real-world entities — **Things**, **People**, **Environments** — are *"represented by some
form of EN wrapper"*. All of that is literal here:

* `Sensor`, `Actuator`, `RfidGate` — Connected Devices.
* `SmartObject` — a PLC. Per §3.1 exactly two Platform Components run on such a device, and both
  are wired in: a Field-Tier PDP and Field-Tier Ledger Clients. It has a small ring buffer and no
  persistence, because *"Smart Objects are controller boards … but lack any (usable) local
  storage"*.
* `RealWorldEntity` — a conveyor (Thing) wrapped by the PLC, ambient temperature (Environment)
  wrapped by an IoT sensor, operator proximity (People) wrapped by an RFID gate. These are the
  paper's own three examples.

`SmartObject.on_sample()` is the on-board control loop: if a signal passes a hard interlock the
machine is stopped **on the device**, with no network involved at all. That is what the paper means
by *"a semi-autonomous entity that does not need to interact too frequently with the upper
layers"*.

`SmartObject.handle_command()` is the other half: every incoming command is first put to the PDP,
*"approving their execution only if they are legitimate"*.

---

## 4. `faredge/edge/` — the Edge Tier, seven Components

`gateway.py` instantiates all seven and lists them in `COMPONENTS`. Walking them in data-flow
order:

### `field_abstraction.py` (Component 3)

Keeps a local registry of Field device endpoints and translates between logical device ids and
whatever the protocol adapter uses. `ingest()` accepts `Event` and `DataMessage` and **raises
`StreamNotHandledError` on a `StreamRecord`** — the separation of concerns from the paper is
enforced by the type system rather than left as a comment. `send_command()` goes the other way:
PDP check, then adapter write, with latency recorded.

### `data_routing.py` (Component 4)

Implements *"the capability of establishing direct producer-consumer channels on demand"* (§2.2).
`open_channel()` creates a `Channel` with a declared destination (`EDGE` or `CLOUD`) and a pipeline
of composable stages:

* `DeadBandFilter` — drop a reading that repeats the previous value within a relative tolerance.
* `Decimator` — forward every *n*-th record.
* `WindowAggregator` — replace a window of records by mean/min/max per signal.
* `ThresholdDetector` — raise an `Event` when a signal leaves its band (the one thing that must
  travel immediately).

Each channel counts bytes in and bytes out, so the filtering ratio is a measured quantity. The
gateway opens two channels: a cheap one to local analytics, and a heavily filtered one to the
Cloud — *"filtering, in order to save network resources, when the final destination of the
information flow is in the Cloud"*.

### `analytics_engine.py` (Component 2, an Enabler)

The paper describes this Enabler as *"an execution engine — i.e., a generic container for running
data processing logic — and southbound/northbound interfaces"*. The code has exactly that shape:

```python
engine.deploy(algorithm)      # pluggable logic
engine.southbound(record)     # raw field data in
engine.northbound(publisher)  # processed results out, to the Ledger Tier
```

Two algorithms ship with it:

* `StationKpiAlgorithm` — the standard OEE decomposition computed incrementally on one station:

  ```
  availability = run_time / planned_time
  performance  = ideal_cycle_time × parts_total / run_time
  quality      = parts_good / parts_total
  OEE          = availability × performance × quality
  ```

  plus throughput, mean cycle time, energy, energy per part and vibration RMS. Its `results()`
  maps each local KPI onto a global KPI **and the rule by which local values combine** — `sum`,
  `min`, `max`, `weighted_mean` or `ratio`.

* `EwmaAnomalyDetector` — an exponentially weighted mean and variance with a k-sigma test. This
  runs next to the machine, which is what makes a sub-millisecond detect-and-react loop possible.

`northbound()` applies a dead band before publishing: a KPI is only sent to the ledger when it has
actually moved. This keeps the ledger in the regime the paper says it is good at — frequent reads,
infrequent writes.

### `automation_services.py` (Component 1, an Enabler)

*"A runtime environment for local automation workflows"* whose core services are *"security and a
local registry of Field device endpoints"*, with interfaces standard enough that *"edge automation
workflows developed on a given Platform [can] be easily migrated to another"*.

The portability requirement is met by making `WorkflowContext` the **only** thing a workflow may
touch: `ctx.devices()`, `ctx.command()`, `ctx.may()`, `ctx.ledger`, `ctx.log()`. No workflow
imports a gateway, an adapter or a peer. Three workflows ship:

* **`StationCycleWorkflow`** — one station's share of a line-wide process. It acquires the station
  lock through Orchestration Services, claims each part, and reports completion. Every instance
  runs on a different gateway and knows nothing about the others; all coordination is ledger state.
* **`SafetyReactionWorkflow`** — subscribes to locally detected anomalies and throttles or stops the
  station. This is the paper's *"Automation is controlled — to some extent — by the results of
  Analytics"*, closed entirely inside the gateway.
* **`ReconfigurationWorkflow`** — subscribes to `ParameterChanged` ledger events and applies the
  new setpoints to the field. Because the change was agreed by consensus, every gateway converges
  on the same configuration with no central push.

### `node_management.py` (Component 7)

*"Technical hooks — e.g., service and/or subscription endpoints — that allow for the remote
management of EGs"*: named endpoints (`getStatus`, `heartbeat`, `getMetrics`, `commission`,
`decommission`) invocable by the Cloud Tier's Platform Management, plus a subscription for pushing
status upward.

### `gateway.py` — putting them together

`EdgeGateway.__init__` builds its Field Tier (PLC, sensors, RFID gate, drive, and the real-world
entities they wrap), instantiates all seven Components, and wires them:

```
adapter ──┬── StreamRecord ──▶ Data Routing ──┬──▶ Analytics Engine
          │                                   └──▶ WAN ──▶ Cloud Data Storage
          └── Event/DataMessage ──▶ Field Abstraction ──▶ Automation workflows

Ledger events ──▶ Ledger Clients ──▶ Automation workflows
```

`feed(row)` is the hot path for one field sample: the Smart Object runs its interlock, the sensor
publishes the bundle, and a completed operation is announced as an Event.
`publish_kpis()` and `synchronise_model()` are the northbound paths to the Ledger Tier.

---

## 5. `faredge/ledger/` — the Ledger Tier

This is the paper's distinctive contribution: *"a specific system layer — the Ledger Tier — that
implements such mechanisms"* for keeping global variables in sync across parallel local instances
without any central service.

### `blockchain.py` — blocks and state

A `Block` carries its index, the previous block's hash, its transactions, a Merkle root, a
**state root** (the hash of the whole world state *after* executing the block), the proposer's
address and signature. `WorldState` is `{service_name: {key: value}}` with a deterministic root
hash.

`verify_integrity()` re-derives every Merkle root and every block hash and checks that each block
links to its predecessor. That one method is what makes immutability testable: edit a committed
record and it fails immediately.

### `smart_contract.py` — deterministic business logic

`SmartContract` subclasses declare endpoints and receive `(state, args, caller, t)`. Two rules are
enforced structurally:

1. **Determinism.** A contract gets its notion of "now" from the signed transaction; it has no
   access to a clock or a random source. This is what allows *"each service call [to be] executed
   in parallel by all Peer Nodes"* and their state roots to be comparable.
2. **On-ledger deployment.** `compile_contract(source)` compiles contract source in a restricted
   namespace with no `__import__`, `open` or `eval`. A `DEPLOY_CONTRACT` transaction carries that
   source, so *"even the executable code of Ledger Services can be deployed and updated online by
   means of a distributed ledger transaction"*.

A contract also has a `validate()` hook, which is where business rules live — *"Ledger services …
will be responsible for … enforcing business rules across the computing nodes of a distributed
system"*.

### The four Ledger Services (`ledger/services/`)

**`orchestration.py` — Orchestration Services (Automation Domain).**
Work orders, **distributed mutual exclusion** on each station, and enforcement of the global part
route. `acquireStation` grants a station to exactly one gateway; `claimPart` refuses any part that
is not next in that station's route; `reportOperationComplete` refuses a report for a part the
caller does not hold or that is at a different position. This is the paper's motivating problem —
*"keeping global variables in-sync across all local instances … reaching a consensus among local
instances on a global truth"* — solved without an orchestrator.

**`configuration.py` — Configuration Services (Automation + Management XC).**
A global, consensus-backed asset/service registry (plug-and-play: a device registered at any
gateway is visible platform-wide) and versioned reconfiguration. `setParameter` checks the value
against a safety envelope that is itself shared state, then emits `ParameterChanged`, which every
subscribed gateway applies.

**`data_publishing.py` — Data Publishing Services (Analytics Domain).**
A near-verbatim implementation of the paper's worked example: each gateway submits a local KPI,
and *"the underlying smart contract will compute the new value of the corresponding global KPI"*.
Five aggregation methods are supported — `sum`, `min`, `max`, `weighted_mean`, `ratio` — all of
them order-independent, which matters because transaction order across gateways is not controlled
by anyone. The aggregation logic lives in the contract, not in the cloud and not on any gateway.

**`synchronization.py` — Synchronization Services (Digital Models XC).**
Gateways report observed parameter values; the contract computes the relative drift against the
model and corrects the model (exponentially weighted) only when the drift exceeds the declared
threshold. This implements the paper's own example: *"a machine model assumes a given value of
electric power / energy consumption, but the actual values will diverge as the real machine wears
down. To detect this gap and correct the model accordingly…"*

### `peer_node.py` — one replica

Each `PeerNode` owns a full chain replica, a full world-state replica and **its own instances of
every Ledger Service**. It has a `host_tier` attribute, because the paper stresses the Ledger Tier
is logical and peers *"can be installed in environments belonging to the Cloud or Edge Tiers"*.

Three methods matter:

* `propose(calls)` — execute the batch on a copy of the state, build and sign a block whose state
  root is the hash of the result.
* `validate(block)` — check the proposer's signature, check every client signature, check the block
  extends this peer's head, **independently re-execute the batch**, and vote only if the resulting
  state root matches the proposer's claim.
* `commit(block)` — append and apply.

A peer flagged `byzantine` forges its state root when proposing and inverts its verdict when
validating — which is exactly what the consensus protocol is designed to survive.

### `consensus.py` — the protocol

Three phases, PBFT-flavoured:

```
PRE-PREPARE  proposer executes the batch, signs the block, broadcasts it
PREPARE      every online peer independently re-executes and votes on the state root
COMMIT       on quorum, every honest peer appends; otherwise the next peer proposes
```

Quorum follows the paper's two thresholds:

| operation | required |
|---|---|
| `APPEND`, `DEPLOY_CONTRACT` | strict majority — *"a majority consensus of PNs"* |
| `AMEND`, `REMOVE` | unanimity — *"a system-wide unanimous consensus"* |

The paper does not say majority of *which* set, so `LedgerConfig.quorum_basis` chooses between
`"known"` (all peers; the standard quorum rule that cannot split-brain; the default) and
`"online"` (those currently reachable). With 5 peers the default tolerates 2 crash faults and
outvotes 1 Byzantine peer.

### `dlt.py` — the Distributed Ledger Enabler

The facade the rest of the platform talks to. It owns the peers and the key directory, installs
the four Ledger Services on each peer, and runs a background committer thread that batches
incoming calls into blocks and drives consensus. It offers:

* `submit(call, identity)` → `Receipt` — the write path, asynchronous and batched;
* `read(service, key)` — the cheap path, served by one replica with no consensus;
* `subscribe(service, callback)` — ledger events, for Edge and Cloud Ledger Clients;
* `crash(i)`, `recover(i)`, `make_byzantine(i)` — fault injection;
* `verify_replicas()` — do all honest replicas hold the same chain and state, and is each chain
  internally intact?

---

## 6. `faredge/cloud/` — the Cloud Tier, eleven Components

The paper groups them in three, and so does `cloud_tier.py`:

**Public Cloud Services** — the three Open API Enablers (`open_api.py`). Every request goes
through the same three steps: Identity Management resolves the single-sign-on token, the
Cloud-Tier PDP authorises the `(action, resource)` pair, and the call is served through Cloud
Ledger Clients or the Edge Tier. Request and response bytes are charged to the WAN.

* *Open API for Automation* → Orchestration Services, Configuration Services, Edge Automation
  Services.
* *Open API for Analytics* → Data Publishing Services, Edge Analytics Engine, Data Storage.
* *Open API for Virtualization* → digital models and the simulator.

**Internal Cloud Services** (`internal_services.py`) — `IdentityManagement` (enrolment, SSO
tokens, roles), `ModelRepository` (heavy model artefacts and version history; the authoritative
shared parameters live on the ledger), `DataStorage` (where the filtered cloud-bound stream lands).

**Platform Cloud Tools** — `SecurityManagement` (the Policy Administration Point every PDP
consults), the Cloud PDP, Cloud Ledger Clients, `RealToDigitalSynchronization` (subscribes to
`ModelUpdated` ledger events and mirrors corrected parameters into the Model Repository) and
`PlatformManagement` (drives the Node Management hooks of every gateway).

---

## 7. `faredge/simulation/line_simulator.py` — the Simulation Domain

The paper is firm that this domain is *"only a consumer of the Digital Models XC Functions"*, and
the code respects it: the simulator never writes to the ledger. It reads two things:

* per-machine physical parameters from **Synchronization Services** (the corrected models), and
* plant availability and quality from **Data Publishing Services**, because *"the Automation and
  Simulation domains … can both make direct use of the outcome of data analysis algorithms"*.

The model is a serial flow line: the slowest station sets the pace, availability and quality scale
the good-part rate, and energy is the installed power over the running time.

```
good_rate_pph = 3600 / bottleneck_cycle_time × availability × quality
```

`what_if(overrides)` re-runs the model with changed parameters and reports the throughput delta and
whether the bottleneck moved — *"testing what/if scenarios at minimal cost and without essential
impact on regular shop activities"*.

---

## 8. `faredge/platform.py` — assembly

`FarEdgePlatform.__init__` builds the Ledger Tier first (peers spread deliberately across Edge and
Cloud hosts), then the Cloud Tier, then one Edge Gateway per station — each of which builds its own
Field Tier.

`bootstrap()` is what an integrator would actually do, in order:

1. administer access policies centrally (Security Management);
2. enrol digital identities (Identity Management);
3. define the global route and open a work order (Orchestration Services);
4. declare the parameter safety envelope (Configuration Services);
5. declare the global KPIs and their aggregation rules (Data Publishing Services);
6. commission each gateway — plug-and-play registration of its devices, publication of its digital
   model, deployment of its three workflows;
7. mirror the as-designed models into the Cloud Model Repository.

`run_shift(rows)` starts one thread per Edge Gateway, each feeding its own station's samples, so
the Ledger Tier really does serve concurrent clients. At the end every KPI is force-published and
every model observation reported, so the global view is exact.

---

## 9. `dataset/generate_dataset.py`

Generates the synthetic shop floor described in the README, and — importantly — also computes the
**ground truth**: the same KPIs calculated centrally, in one process, over all raw samples, using
the same formulas. `run_demo.py` compares the distributed result against it, and the two agree to
about 10⁻¹⁵, which is floating-point noise.

---

## 10. `run_demo.py` and `tests/test_platform.py`

`run_demo.py` runs the platform end to end and prints six blocks: the dataset, the platform
inventory, the production run, distributed analytics, distributed automation, digital models and
simulation, and finally integrity, security and traffic. It also performs a live tamper test —
editing a committed record and re-running the chain verification.

The 18 tests cover crypto round trips, the four Ledger Services' business rules, tamper detection,
rejection of unsigned callers, crash tolerance (2 of 5 peers), Byzantine tolerance (1 of 5), the
dead-band filter, the stream/event separation, hand-checked KPI arithmetic, default-deny policy
evaluation, and an end-to-end shift whose global KPIs must match the central calculation to nine
decimal places.
