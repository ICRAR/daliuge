#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2026
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA
#

"""Unit tests for the Stage protocol, Pipeline, and UnrollStage.

The pipeline intentionally accepts structural Stage implementations, so the
tests use small recording stages rather than a second production hierarchy.

Base authored by LarkVenter (branch issue-40-...); additions below fill in
edge cases not yet covered (marked with `# added`).
"""

import unittest
from unittest.mock import patch

from dlg.translator.artefacts import LogicalGraphTemplate, PhysicalGraphTemplate
from dlg.translator.errors import StageException
from dlg.translator.pipeline import Pipeline
from dlg.translator.stages.unroll.stage import UnrollOptions, UnrollStage


class RecordingStage:
    """A structural Stage implementation that records Pipeline interactions."""

    def __init__(self, name, events, run, stamp):
        self.name = name
        self._events = events
        self._run = run
        self._stamp = stamp

    def run(self, artefact):
        self._events.append((self.name, "run", artefact))
        return self._run(artefact)

    def stamp(self, artefact):
        self._events.append((self.name, "stamp", artefact))
        return self._stamp(artefact)


class PipelineTest(unittest.TestCase):
    def test_runs_stages_and_stamps_each_boundary_in_order(self):
        events = []
        first = RecordingStage(
            "first", events, lambda value: value + 1, lambda value: value * 10
        )
        second = RecordingStage(
            "second", events, lambda value: value + 2, lambda value: value + 100
        )

        self.assertEqual(Pipeline([first, second]).run(1), 122)
        self.assertEqual(
            events,
            [
                ("first", "run", 1),
                ("first", "stamp", 2),
                ("second", "run", 20),
                ("second", "stamp", 22),
            ],
        )

    def test_repro_disabled_skips_stamping_but_still_runs_stages(self):
        events = []
        stage = RecordingStage(
            "only", events, lambda value: value + 1, lambda value: value * 10
        )

        self.assertEqual(Pipeline([stage], repro=False).run(1), 2)
        self.assertEqual(events, [("only", "run", 1)])

    def test_then_returns_an_extended_pipeline_without_mutating_the_original(self):
        events = []
        first = RecordingStage(
            "first", events, lambda value: value + 1, lambda value: value
        )
        second = RecordingStage(
            "second", events, lambda value: value * 2, lambda value: value
        )
        original = Pipeline([first], repro=False)
        extended = original.then(second)

        self.assertEqual(original.run(1), 2)
        self.assertEqual(extended.run(1), 4)
        self.assertEqual(
            events,
            [
                ("first", "run", 1),
                ("first", "run", 1),
                ("second", "run", 2),
            ],
        )

    def test_run_failure_is_wrapped_with_the_failing_stage_name(self):
        def fail(_artefact):
            raise ValueError("bad input")

        stage = RecordingStage("unroll", [], fail, lambda artefact: artefact)

        with self.assertRaises(StageException) as context:
            Pipeline([stage]).run("logical graph")

        self.assertEqual(context.exception.stage, "unroll")
        self.assertEqual(str(context.exception), "stage 'unroll' failed: bad input")
        self.assertIsInstance(context.exception.__cause__, ValueError)

    def test_stamp_failure_is_wrapped_with_the_failing_stage_name(self):
        def fail(_artefact):
            raise LookupError("missing reprodata")

        stage = RecordingStage("partition", [], lambda artefact: artefact, fail)

        with self.assertRaises(StageException) as context:
            Pipeline([stage]).run("physical graph template")

        self.assertEqual(context.exception.stage, "partition")
        self.assertEqual(
            str(context.exception), "stage 'partition' failed: missing reprodata"
        )
        self.assertIsInstance(context.exception.__cause__, LookupError)

    # added: empty pipeline is a no-op that returns the same object
    def test_empty_pipeline_returns_input_unchanged(self):
        sentinel = object()
        self.assertIs(Pipeline([]).run(sentinel), sentinel)

    # added: a stage after the failing one must never be touched at all
    def test_first_failing_stage_short_circuits_later_stages(self):
        events = []

        def fail(_value):
            raise ValueError("boom")

        first = RecordingStage("bad", events, fail, lambda value: value)
        second = RecordingStage("never", events, lambda value: value, lambda value: value)

        with self.assertRaises(StageException):
            Pipeline([first, second]).run(1)

        self.assertTrue(all(name != "never" for name, _, _ in events))

    # added: with repro=False, stamp must be skipped entirely -- even a
    # stamp that would itself raise must never be invoked
    def test_stamp_skipped_when_repro_false_even_on_failure_path(self):
        events = []

        def failing_stamp(_value):
            raise AssertionError("stamp should never run when repro=False")

        stage = RecordingStage("only", events, lambda value: value + 1, failing_stamp)

        result = Pipeline([stage], repro=False).run(1)

        self.assertEqual(result, 2)
        self.assertEqual(events, [("only", "run", 1)])


# added: StageException's own formatting, tested directly rather than only
# observed through Pipeline's wrapping behaviour
class StageExceptionTest(unittest.TestCase):
    def test_message_included_when_given(self):
        exc = StageException("unroll", "bad input")
        self.assertEqual(exc.stage, "unroll")
        self.assertIn("unroll", str(exc))
        self.assertIn("bad input", str(exc))

    def test_message_omitted_when_not_given(self):
        exc = StageException("unroll")
        self.assertEqual(exc.stage, "unroll")
        self.assertIn("unroll", str(exc))
        # No trailing ": " when there's no message.
        self.assertNotIn(":", str(exc))


class UnrollStageTest(unittest.TestCase):
    @staticmethod
    def logical_graph():
        return {
            "modelData": {"name": "fixture"},
            "nodeDataArray": [{"id": 1, "fields": [{"name": "input"}]}],
            "linkDataArray": [],
            "reprodata": {"rmode": "1"},
        }

    @patch("dlg.translator.stages.unroll.stage.unroll")
    def test_run_delegates_with_options_and_does_not_expose_lgt_storage(
        self, unroll
    ):
        unroll.return_value = [
            {"oid": "session_-1_0", "categoryType": "Application"},
            {"rmode": "1"},
        ]
        logical_graph = LogicalGraphTemplate.from_wire(self.logical_graph())
        before = logical_graph.to_wire()
        stage = UnrollStage(
            UnrollOptions(oid_prefix="session", zerorun=True, app="test.App")
        )

        output = stage.run(logical_graph)

        self.assertIsInstance(output, PhysicalGraphTemplate)
        self.assertEqual(output.to_wire(), unroll.return_value)
        unroll.assert_called_once()
        kwargs = unroll.call_args.kwargs
        self.assertEqual(kwargs["oid_prefix"], "session")
        self.assertTrue(kwargs["zerorun"])
        self.assertEqual(kwargs["app"], "test.App")
        self.assertEqual(kwargs["lg"], before)

        kwargs["lg"]["nodeDataArray"][0]["fields"].append({"name": "changed"})
        self.assertEqual(logical_graph.to_wire(), before)

    # added: the default (no options given) path was only ever exercised
    # implicitly -- assert it explicitly so a broken default can't slip by
    @patch("dlg.translator.stages.unroll.stage.unroll")
    def test_run_uses_defaults_when_no_options_given(self, unroll):
        unroll.return_value = [{"oid": "a"}, {}]
        logical_graph = LogicalGraphTemplate.from_wire(self.logical_graph())

        UnrollStage().run(logical_graph)

        kwargs = unroll.call_args.kwargs
        self.assertIsNone(kwargs["oid_prefix"])
        self.assertFalse(kwargs["zerorun"])
        self.assertIsNone(kwargs["app"])

    @patch("dlg.translator.stages.unroll.stage.init_pgt_unroll_repro_data")
    def test_stamp_wraps_the_repro_hook_result_and_keeps_input_isolated(self, hook):
        pgt = PhysicalGraphTemplate.from_wire(
            [
                {"oid": "session_-1_0", "categoryType": "Application"},
                {"rmode": "1"},
            ]
        )
        before = pgt.to_wire()
        hook.return_value = [
            {
                "oid": "session_-1_0",
                "categoryType": "Application",
                "reprodata": {"merkleroot": "abc"},
            },
            {"rmode": "1"},
        ]

        stamped = UnrollStage().stamp(pgt)

        self.assertIsInstance(stamped, PhysicalGraphTemplate)
        self.assertEqual(stamped.to_wire(), hook.return_value)
        hook.assert_called_once_with(before)
        hook_input = hook.call_args.args[0]
        hook_input[0]["oid"] = "changed"
        self.assertEqual(pgt.to_wire(), before)

    # added: the class-level `name` used in StageException messages
    def test_name_is_unroll(self):
        self.assertEqual(UnrollStage.name, "unroll")


if __name__ == "__main__":
    unittest.main()
