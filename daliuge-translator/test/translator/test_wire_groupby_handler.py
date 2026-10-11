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

import unittest
from collections import defaultdict
from types import SimpleNamespace
from unittest.mock import Mock

from dlg.common import dropdict

import dlg.translator.stages.unroll.wire as wire_module
from dlg.translator.stages.unroll.coordinate import InstanceId
from dlg.translator.vocabulary import Categories


def _with_coord(drop, coord):
    drop.coord = coord
    return drop


class FakeDrop(dict):
    """Small DROP stub that records physical wiring calls."""

    def __init__(self, **values):
        super().__init__(**values)
        self.addConsumer = Mock()
        self.addInput = Mock()


class TestGroupByHandlerWiring(unittest.TestCase):

    def test_wire_uses_groupby_pairs_and_preserves_port_names(self):
        source = SimpleNamespace(
            id="source",
            name="source",
            category=Categories.FILE,
            is_group=False,
            h_level=1,
            gid="scatter",
            group=None,
            jd={
                "category": Categories.FILE,
                "categoryType": "Data",
            },
            getPortName=Mock(return_value="source-output"),
        )

        target = SimpleNamespace(
            id="groupby",
            name="groupby",
            category=Categories.GROUP_BY,
            is_group=True,
            h_level=0,
            gid="groupby",
            group=None,
            group_keys=None,
            groupby_width=1,
            group_by_scatter_layers=(2, [], []),
            jd={
                "category": Categories.GROUP_BY,
                "categoryType": "Data",
            },
            getPortName=Mock(return_value="target-input"),
        )

        # Deliberately reverse the IID order so the test checks that
        # GroupByHandler pairing is actually used by wire().
        source_drops = [
            _with_coord(
                FakeDrop(oid="source-1", iid="1"),
                InstanceId((1,)),
            ),
            _with_coord(
                FakeDrop(oid="source-0", iid="0"),
                InstanceId((0,)),
            ),
        ]
        target_drops = [
            FakeDrop(oid="groupby-0"),
            FakeDrop(oid="groupby-1"),
        ]

        graph = SimpleNamespace(
            _session_id="session-",
            _done_dict={
                source.id: source,
                target.id: target,
            },
            _drop_dict={
                source.id: source_drops,
                target.id: target_drops,
                "new_added": [],
            },
            _lg_links=[
                {
                    "from": source.id,
                    "to": target.id,
                    "fromPort": "source-port",
                    "toPort": "target-port",
                }
            ],
            _loop_aware_set=set(),
        )

        wire_module.wire(graph)

        # IID 0 must pair with target 0, and IID 1 with target 1.
        source_drops[1].addConsumer.assert_called_once_with(
            target_drops[0],
            name="source-output",
        )
        target_drops[0].addInput.assert_called_once_with(
            source_drops[1],
            name="target-input",
        )

        source_drops[0].addConsumer.assert_called_once_with(
            target_drops[1],
            name="source-output",
        )
        target_drops[1].addInput.assert_called_once_with(
            source_drops[0],
            name="target-input",
        )

        source.getPortName.assert_any_call(
            "outputPorts",
            portId="source-port",
        )
        target.getPortName.assert_any_call(
            "inputPorts",
            portId="target-port",
        )


    def test_wire_records_outputs_producers_and_port_map(self):
        source_port_names = {
            "source-output-id": "source-output",
        }
        target_port_names = {
            "target-input-id": "target-input",
        }

        def get_port_name(port_names, ports, index=0, portId=None):
            del ports

            if index < 0:
                return port_names

            return port_names.get(portId)

        source = SimpleNamespace(
            id="source",
            name="source",
            category=Categories.PYTHON_APP,
            categoryType="Application",
            jd={
                "categoryType": "Application",
            },
            is_group=False,
            is_start_node=False,
            group=None,
            h_level=0,
            getPortName=lambda ports, index=0, portId=None: get_port_name(
                source_port_names,
                ports,
                index,
                portId,
            ),
        )

        target = SimpleNamespace(
            id="groupby",
            name="groupby",
            category=Categories.GROUP_BY,
            categoryType="Construct",
            jd={
                "category": Categories.GROUP_BY,
                "categoryType": "Construct",
            },
            is_group=True,
            group_keys=None,
            group_by_scatter_layers=(3, [], []),
            groupby_width=3,
            h_level=0,
            getPortName=lambda ports, index=0, portId=None: get_port_name(
                target_port_names,
                ports,
                index,
                portId,
            ),
        )

        source_drops = [
            _with_coord(
                dropdict({
                    "oid": "source-10",
                    "iid": "0-10",
                }),
                InstanceId((0, 10)),
            ),
            _with_coord(
                dropdict({
                    "oid": "source-2",
                    "iid": "0-2",
                }),
                InstanceId((0, 2)),
            ),
            _with_coord(
                dropdict({
                    "oid": "source-11",
                    "iid": "0-11",
                }),
                InstanceId((0, 11)),
            ),
        ]

        target_drops = [
            dropdict({"oid": "group-0"}),
            dropdict({"oid": "group-1"}),
            dropdict({"oid": "group-2"}),
        ]

        logical_link = {
            "from": source.id,
            "to": target.id,
            "fromPort": "source-output-id",
            "toPort": "target-input-id",
        }

        graph = SimpleNamespace(
            _session_id="test",
            _drop_dict=defaultdict(
                list,
                {
                    source.id: source_drops,
                    target.id: target_drops,
                },
            ),
            _done_dict={
                source.id: source,
                target.id: target,
            },
            _lg_links=[logical_link],
            _loop_aware_set=set(),
        )

        wire_module.wire(graph)

        self.assertEqual(
            [
                [{"group-1": "source-output"}],
                [{"group-0": "source-output"}],
                [{"group-2": "source-output"}],
            ],
            [
                source_drop["outputs"]
                for source_drop in source_drops
            ],
        )

        self.assertEqual(
            [
                [{"source-2": "target-input"}],
                [{"source-10": "target-input"}],
                [{"source-11": "target-input"}],
            ],
            [
                target_drop["producers"]
                for target_drop in target_drops
            ],
        )

        self.assertEqual(
            [
                {"target-input": "source-output"},
                {"target-input": "source-output"},
                {"target-input": "source-output"},
            ],
            [
                target_drop["port_map"]
                for target_drop in target_drops
            ],
        )


if __name__ == "__main__":
    unittest.main()
