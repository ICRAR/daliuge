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

"""Focused tests for partition graph linearisation."""

import unittest

import networkx as nx

from dlg.common import CategoryType, dropdict
from dlg.translator.stages.partition.linearise import linearise
from dlg.translator.stages.partition.pgt import PGT
from dlg.translator.stages.partition.pgtp import (
    MinNumPartsPGTP,
    PSOPGTP,
)


def _drop(oid, category_type):
    return dropdict(
        {
            "oid": oid,
            "categoryType": category_type,
            "name": oid,
            "weight": 1,
        }
    )


def _dag(source, target, drop_type):
    dag = nx.DiGraph()
    dag.add_node(
        1,
        weight=1,
        drop_type=drop_type,
        drop_spec=source,
        gid=4,
    )
    dag.add_node(
        2,
        weight=1,
        drop_type=drop_type,
        drop_spec=target,
        gid=7,
    )
    dag.add_edge(1, 2, weight=1)
    return dag


class TestPartitionLinearise(unittest.TestCase):
    def test_application_edge_gets_intermediate_data_drop(self):
        source = _drop("source", CategoryType.APPLICATION)
        target = _drop("target", CategoryType.APPLICATION)
        dag = _dag(source, target, 1)

        extra_drops, links = linearise(
            [source, target],
            dag,
            {
                "source": 1,
                "target": 2,
            },
        )

        self.assertEqual(len(extra_drops), 1)
        self.assertEqual(
            extra_drops[0]["oid"],
            "source_TransData_0",
        )
        self.assertEqual(
            extra_drops[0]["categoryType"],
            CategoryType.DATA,
        )
        self.assertEqual(
            extra_drops[0]["dropclass"],
            "dlg.data.drops.memory.InMemoryDROP",
        )

        self.assertEqual(
            links,
            [
                {"from": -1, "to": 2},
                {"from": 1, "to": -1},
            ],
        )
        self.assertEqual(
            set(dag.edges()),
            {
                (1, -1),
                (-1, 2),
            },
        )
        self.assertEqual(dag.nodes[-1]["gid"], 7)

    def test_data_edge_gets_intermediate_application_drop(self):
        source = _drop("source", CategoryType.DATA)
        target = _drop("target", CategoryType.DATA)
        dag = _dag(source, target, 0)

        extra_drops, links = linearise(
            [source, target],
            dag,
            {
                "source": 1,
                "target": 2,
            },
        )

        self.assertEqual(len(extra_drops), 1)
        self.assertEqual(
            extra_drops[0]["oid"],
            "source_TransApp_0",
        )
        self.assertEqual(
            extra_drops[0]["categoryType"],
            CategoryType.APPLICATION,
        )
        self.assertEqual(
            extra_drops[0]["dropclass"],
            "dlg.drop.BarrierAppDROP",
        )
        self.assertEqual(
            links,
            [
                {"from": -1, "to": 2},
                {"from": 1, "to": -1},
            ],
        )

    def test_mixed_edge_does_not_create_synthetic_drop(self):
        source = _drop("source", CategoryType.APPLICATION)
        target = _drop("target", CategoryType.DATA)

        dag = nx.DiGraph()
        dag.add_node(
            1,
            weight=1,
            drop_type=1,
            drop_spec=source,
        )
        dag.add_node(
            2,
            weight=1,
            drop_type=0,
            drop_spec=target,
        )
        dag.add_edge(1, 2, weight=1)

        extra_drops, links = linearise(
            [source, target],
            dag,
            {
                "source": 1,
                "target": 2,
            },
        )

        self.assertEqual(extra_drops, [])
        self.assertEqual(
            links,
            [{"from": 1, "to": 2}],
        )
        self.assertEqual(
            set(dag.edges()),
            {(1, 2)},
        )

    def test_gojs_facade_does_not_trigger_linearisation(self):
        source = _drop("source", CategoryType.APPLICATION)
        target = _drop("target", CategoryType.APPLICATION)
        dag = _dag(source, target, 1)

        pgt = PGT([source, target], build_dag=False)
        pgt._dag = dag
        pgt._extra_drops = None

        pgt.to_gojs_json(
            string_rep=False,
            visual=True,
        )

        self.assertIsNone(pgt._extra_drops)
        self.assertEqual(
            set(dag.edges()),
            {(1, 2)},
        )

    def test_linearising_algorithms_own_visual_preparation(self):
        for pgtp_type in (MinNumPartsPGTP, PSOPGTP):
            with self.subTest(pgtp=pgtp_type.__name__):
                source = _drop(
                    "source",
                    CategoryType.APPLICATION,
                )
                target = _drop(
                    "target",
                    CategoryType.APPLICATION,
                )
                dag = _dag(source, target, 1)

                pgtp = object.__new__(pgtp_type)
                pgtp._drop_list = [source, target]
                pgtp._dag = dag
                pgtp._extra_drops = None
                pgtp._links = []

                pgtp._prepare_gojs_projection()

                self.assertEqual(
                    len(pgtp._extra_drops),
                    1,
                )
                self.assertEqual(
                    pgtp._extra_drops[0]["oid"],
                    "source_TransData_0",
                )
                self.assertEqual(
                    set(dag.edges()),
                    {
                        (1, -1),
                        (-1, 2),
                    },
                )


if __name__ == "__main__":
    unittest.main()
