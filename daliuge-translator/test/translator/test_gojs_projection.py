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

"""Tests for the read-only GoJS partition projection."""

from copy import deepcopy
import unittest

from dlg.common import CategoryType
from dlg.translator.stages.partition.projections.gojs import project_gojs


class TestGojsProjection(unittest.TestCase):
    def test_projection_preserves_payload_shape_without_mutating_inputs(self):
        drops = [
            {
                "oid": "app",
                "categoryType": CategoryType.APPLICATION,
                "name": "app",
                "iid": 0,
            },
            {
                "oid": "data",
                "categoryType": CategoryType.DATA,
                "name": "data",
                "iid": 1,
            },
        ]
        extra_drops = [
            {
                "oid": "extra-data",
                "categoryType": CategoryType.DATA,
                "name": "go_data",
                "iid": 2,
            },
            {
                "oid": "extra-app",
                "categoryType": CategoryType.APPLICATION,
                "name": "go_app",
                "iid": 3,
            },
        ]
        links = [
            {"from": 1, "to": -1},
            {"from": -1, "to": 2},
        ]

        drops_before = deepcopy(drops)
        extra_before = deepcopy(extra_drops)
        links_before = deepcopy(links)

        result = project_gojs(
            drops,
            extra_drops,
            links,
        )

        self.assertEqual(
            result,
            {
                "class": "go.GraphLinksModel",
                "nodeDataArray": [
                    {
                        "key": 1,
                        "oid": "app",
                        "category": "Application",
                        "name": "app",
                        "iid": 0,
                    },
                    {
                        "key": 2,
                        "oid": "data",
                        "category": "Data",
                        "name": "data",
                        "iid": 1,
                    },
                    {
                        "key": -1,
                        "oid": "extra-data",
                        "category": "Data",
                        "name": "go_data",
                        "iid": 2,
                    },
                    {
                        "key": -2,
                        "oid": "extra-app",
                        "category": "PythonApp",
                        "name": "go_app",
                        "iid": 3,
                    },
                ],
                "linkDataArray": links,
            },
        )

        self.assertEqual(drops, drops_before)
        self.assertEqual(extra_drops, extra_before)
        self.assertEqual(links, links_before)

    def test_missing_iid_uses_zero_without_mutating_drop(self):
        drops = [
            {
                "oid": "app",
                "categoryType": CategoryType.APPLICATION,
                "name": "app",
            },
        ]

        result = project_gojs(
            drops,
            [],
            [],
        )

        self.assertEqual(
            result["nodeDataArray"][0]["iid"],
            0,
        )
        self.assertNotIn("iid", drops[0])


if __name__ == "__main__":
    unittest.main()
