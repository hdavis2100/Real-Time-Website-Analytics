from __future__ import annotations

import unittest

from simulator.agents import build_agent
from simulator.personas import BUILTIN_PERSONAS, compile_persona


class PersonaCompilerTest(unittest.TestCase):
    def test_builtin_presets_are_distinct_and_accessible(self) -> None:
        expected = {"nit", "tag", "lag", "calling_station", "maniac", "shove_bot"}
        self.assertTrue(expected.issubset(BUILTIN_PERSONAS.keys()))
        self.assertLess(BUILTIN_PERSONAS["nit"].parameters.preflop_open_bias, BUILTIN_PERSONAS["lag"].parameters.preflop_open_bias)
        self.assertGreater(BUILTIN_PERSONAS["maniac"].parameters.raise_bias, BUILTIN_PERSONAS["tag"].parameters.raise_bias)
        self.assertGreater(BUILTIN_PERSONAS["calling_station"].parameters.cold_call_bias, BUILTIN_PERSONAS["nit"].parameters.cold_call_bias)
        self.assertGreater(BUILTIN_PERSONAS["shove_bot"].parameters.jam_bias, BUILTIN_PERSONAS["tag"].parameters.jam_bias)

    def test_natural_language_compilation_maps_to_expected_style(self) -> None:
        tight_passive = compile_persona("tight preflop, passive postflop")
        sticky = compile_persona("calls too much, hates folding")
        bluffy = compile_persona("very aggressive, over-bluffs rivers")

        self.assertLess(tight_passive.parameters.preflop_open_bias, 0.30)
        self.assertLess(tight_passive.parameters.raise_bias, 0.30)
        self.assertGreater(tight_passive.parameters.fold_threshold, 0.55)
        self.assertLess(tight_passive.parameters.cbet_bias, 0.30)

        self.assertGreater(sticky.parameters.cold_call_bias, 0.60)
        self.assertGreater(sticky.parameters.showdown_tendency, 0.50)
        self.assertGreater(sticky.parameters.fold_threshold, 0.60)

        self.assertGreater(bluffy.parameters.bluff_river_bias, 0.20)
        self.assertGreater(bluffy.parameters.raise_bias, tight_passive.parameters.raise_bias)

    def test_cluster_clone_placeholder_falls_back_gracefully(self) -> None:
        agent = build_agent("cluster_clone", "balanced", agent_id="agent_1")
        self.assertEqual(agent.backend_type, "cluster_clone_placeholder")
        self.assertEqual(agent.agent_id, "agent_1")


if __name__ == "__main__":
    unittest.main()
