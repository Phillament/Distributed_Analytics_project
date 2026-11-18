"""Basic tests for the FAR-EDGE reference implementation.

Run with:
    python -m unittest discover -s tests -v
"""
from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from faredge.common.crypto import Identity, merkle_root, verify
from faredge.common.messages import ServiceCall, StreamRecord
from faredge.common.pdp import PERMIT, Policy, PolicyAdministrationPoint, PolicyDecisionPoint
from faredge.config import PlatformConfig
from faredge.edge.analytics_engine import StationKpiAlgorithm
from faredge.edge.data_routing import DeadBandFilter
from faredge.edge.field_abstraction import StreamNotHandledError
from faredge.platform import FarEdgePlatform


def small_platform(**overrides):
    cfg = PlatformConfig(n_stations=2)
    cfg.network.real_sleep = False
    cfg.ledger.rsa_key_bits = 256
    for key, value in overrides.items():
        setattr(cfg.ledger, key, value)
    return FarEdgePlatform(cfg)


class TestCrypto(unittest.TestCase):
    def test_signature_roundtrip(self):
        identity = Identity("peer", bits=256)
        payload = {"a": 1, "b": [2, 3]}
        signature = identity.sign(payload)
        self.assertTrue(verify(identity.public, payload, signature))
        self.assertFalse(verify(identity.public, {"a": 2}, signature))

    def test_merkle_root_changes_with_content(self):
        self.assertNotEqual(merkle_root(["a", "b"]), merkle_root(["a", "c"]))


class TestLedger(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.platform = small_platform()
        cls.platform.bootstrap("WO-TEST", qty=50)
        cls.ledger = cls.platform.ledger
        cls.client = cls.platform.cloud.ledger_clients

    @classmethod
    def tearDownClass(cls):
        cls.platform.shutdown()

    def test_four_ledger_services_installed(self):
        self.assertEqual(len(self.ledger.installed_services()), 4)

    def test_replicas_agree_and_chain_is_intact(self):
        state = self.ledger.verify_replicas()
        self.assertTrue(state["chains_identical"])
        self.assertTrue(state["states_identical"])
        self.assertTrue(state["all_chains_intact"])

    def test_station_lock_is_mutually_exclusive(self):
        # Bootstrap already gave ST1 to its own gateway, so a second claimant
        # must be refused by the contract.
        taken = self.client.orchestration.acquire_station("ST1", "EG-OTHER")
        self.assertFalse(taken.accepted)
        self.assertIn("already held", taken.reason)

        # ... and an unheld station is granted to the first caller only.
        self.assertTrue(
            self.client.orchestration.acquire_station("ST-SPARE", "EG-A").accepted)
        second = self.client.orchestration.acquire_station("ST-SPARE", "EG-B")
        self.assertFalse(second.accepted)
        self.assertTrue(
            self.client.orchestration.release_station("ST-SPARE", "EG-A").accepted)

    def test_parameter_outside_envelope_is_rejected(self):
        self.assertTrue(
            self.client.configuration.set_parameter("ST1", "speed_factor", 0.8).accepted)
        bad = self.client.configuration.set_parameter("ST1", "speed_factor", 9.9)
        self.assertFalse(bad.accepted)

    def test_kpi_aggregation_methods(self):
        self.client.data_publishing.declare_kpi("t_sum", "sum")
        self.client.data_publishing.submit_local_kpi("EG-A", "t_sum", 10.0)
        self.client.data_publishing.submit_local_kpi("EG-B", "t_sum", 15.0)
        self.assertAlmostEqual(
            self.client.data_publishing.global_kpis()["t_sum"]["value"], 25.0)

        self.client.data_publishing.declare_kpi("t_wm", "weighted_mean")
        self.client.data_publishing.submit_local_kpi("EG-A", "t_wm", 1.0, weight=1.0)
        self.client.data_publishing.submit_local_kpi("EG-B", "t_wm", 0.0, weight=3.0)
        self.assertAlmostEqual(
            self.client.data_publishing.global_kpis()["t_wm"]["value"], 0.25)

    def test_model_drift_correction(self):
        self.client.synchronization.register_model(
            "M1", params={"p": 10.0}, thresholds={"p": 0.05})
        small = self.client.synchronization.report_observation("M1", "p", 10.2)
        self.assertFalse(small.result["updated"])
        big = self.client.synchronization.report_observation("M1", "p", 14.0)
        self.assertTrue(big.result["updated"])

    def test_tampering_breaks_the_hash_chain(self):
        peer = self.ledger.peers[0]
        block = peer.chain.blocks[1]
        block.calls[0]["call"]["args"]["evil"] = True
        ok, reason = peer.chain.verify_integrity()
        self.assertFalse(ok)
        self.assertIn("merkle", reason)
        del block.calls[0]["call"]["args"]["evil"]
        self.assertTrue(peer.chain.verify_integrity()[0])

    def test_unsigned_caller_is_rejected(self):
        stranger = Identity("stranger", bits=256)     # never registered
        call = ServiceCall("ConfigurationServices", "setParameter",
                           {"station": "ST1", "key": "speed_factor", "value": 0.5},
                           stranger.address)
        self.assertFalse(self.ledger.submit(call, stranger).accepted)


class TestFaultTolerance(unittest.TestCase):
    def test_majority_survives_crashed_peers(self):
        platform = small_platform()
        platform.bootstrap("WO-FT", qty=10)
        ledger, client = platform.ledger, platform.cloud.ledger_clients
        try:
            ledger.crash(0)
            ledger.crash(1)                       # 3 of 5 still online
            self.assertTrue(
                client.configuration.set_parameter("ST1", "speed_factor", 0.7).accepted)
            ledger.crash(2)                       # only 2 online: no majority
            self.assertFalse(
                client.configuration.set_parameter("ST1", "speed_factor", 0.6).accepted)
            ledger.recover(0)
            ledger.recover(1)
            ledger.recover(2)
            self.assertTrue(ledger.verify_replicas()["chains_identical"])
        finally:
            platform.shutdown()

    def test_byzantine_peer_is_outvoted(self):
        platform = small_platform()
        platform.bootstrap("WO-BZ", qty=10)
        try:
            platform.ledger.make_byzantine(4)
            receipt = platform.cloud.ledger_clients.configuration.set_parameter(
                "ST1", "speed_factor", 0.55)
            self.assertTrue(receipt.accepted)
            self.assertGreaterEqual(receipt.votes_for, platform.ledger.quorum_for())
        finally:
            platform.shutdown()


class TestEdgeTier(unittest.TestCase):
    def test_dead_band_filter_drops_unchanged_samples(self):
        flt = DeadBandFilter(0.05)
        first = flt(StreamRecord(source="s", channel="raw", values={"v": 10.0}))
        repeat = flt(StreamRecord(source="s", channel="raw", values={"v": 10.1}))
        moved = flt(StreamRecord(source="s", channel="raw", values={"v": 20.0}))
        self.assertIsNotNone(first)
        self.assertIsNone(repeat)
        self.assertIsNotNone(moved)

    def test_field_abstraction_refuses_streams(self):
        platform = small_platform()
        try:
            gateway = next(iter(platform.gateways.values()))
            with self.assertRaises(StreamNotHandledError):
                gateway.field_abstraction.ingest(
                    StreamRecord(source="s", channel="raw", values={"v": 1.0}))
        finally:
            platform.shutdown()

    def test_station_kpi_matches_hand_calculation(self):
        algorithm = StationKpiAlgorithm("ST1", sample_rate_hz=1.0,
                                        ideal_cycle_time_s=2.0)
        for i in range(10):
            algorithm.consume(StreamRecord(
                source="ST1-SENS", channel="raw",
                values={"running": 1.0, "power_kw": 3.6,
                        "part_done": 1.0 if i % 2 else 0.0,
                        "part_ok": 1.0, "cycle_time_s": 2.0}))
        self.assertEqual(algorithm.parts_total, 5)
        self.assertAlmostEqual(algorithm.availability, 1.0)
        self.assertAlmostEqual(algorithm.quality, 1.0)
        self.assertAlmostEqual(algorithm.performance, 1.0)       # 2 s x 5 / 10 s
        self.assertAlmostEqual(algorithm.energy_kwh, 3.6 * 10 / 3600.0)


class TestSecurity(unittest.TestCase):
    def test_default_deny_and_explicit_permit(self):
        pap = PolicyAdministrationPoint()
        pdp = PolicyDecisionPoint("EG", "EDGE", pap)
        self.assertFalse(pdp.evaluate("eve", "stop", "ST1/DRV").permitted)
        pap.put(Policy("p1", ("bob",), ("stop",), ("ST1/*",), PERMIT))
        self.assertTrue(pdp.evaluate("bob", "stop", "ST1/DRV").permitted)
        self.assertFalse(pdp.evaluate("bob", "stop", "ST2/DRV").permitted)

    def test_field_pdp_cache_is_smaller_than_edge_pdp_cache(self):
        pap = PolicyAdministrationPoint()
        self.assertLess(PolicyDecisionPoint("plc", "FIELD", pap).capacity,
                        PolicyDecisionPoint("eg", "EDGE", pap).capacity)


class TestEndToEnd(unittest.TestCase):
    def test_short_shift_produces_consistent_global_kpis(self):
        from dataset.generate_dataset import ShiftGenerator, default_specs, ground_truth

        specs = default_specs()[:2]
        rows = ShiftGenerator(specs, duration_s=60.0, sample_rate_hz=10.0).generate()
        truth = ground_truth(rows, 10.0, {"ST1": 10.0, "ST2": 11.0})

        platform = small_platform()
        try:
            platform.bootstrap("WO-E2E", qty=500)
            platform.run_shift(rows)
            produced = platform.global_kpis()
            for kpi, expected in truth["global"].items():
                if kpi not in produced:
                    continue
                self.assertAlmostEqual(produced[kpi]["value"], expected, places=9,
                                       msg=f"{kpi} diverged")
            self.assertTrue(platform.ledger.verify_replicas()["states_identical"])
        finally:
            platform.shutdown()


if __name__ == "__main__":
    unittest.main(verbosity=2)
