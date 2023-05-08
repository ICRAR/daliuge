#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2019
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
"""
Module tests the helm chart translation and deployment functionality.
"""
import json
import os
import sys
import tempfile
import unittest

import yaml
from dlg.common.k8s_utils import check_k8s_env

if check_k8s_env():
    from dlg.common.version import version as dlg_version
    from dlg.deploy.helm_client import HelmClient


@unittest.skipIf(
    sys.version_info <= (3, 8), "Copying temp files fail on Python < 3.7"
)
@unittest.skipIf(not check_k8s_env(), "K8s is not available")
class TestHelmClient(unittest.TestCase):
    def test_create_default_helm_chart(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            helm_client = HelmClient(
                deploy_dir=tmp_dir, deploy_name="my_fun_name"
            )
            helm_client.create_helm_chart("[]")
            chart_file_name = os.path.join(
                helm_client._chart_dir, "Chart.yaml"
            )
            with open(chart_file_name, "r", encoding="utf-8") as chart_file:
                chart_data = yaml.safe_load(chart_file)
                self.assertEqual(helm_client._chart_name, chart_data["name"])
                self.assertEqual(dlg_version, chart_data["appVersion"])

    @unittest.skip
    def test_custom_ports(self):
        pass

    def test_create_single_node_helm_chart(self):
        pg = [
            {
                "oid": "A",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
            },
            {
                "oid": "B",
                "categoryType": "Application",
                "dropclass": "dlg.apps.simple.SleepApp",
                "inputs": ["A"],
                "outputs": ["C"],
            },
            {
                "oid": "C",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
            },
        ]
        for drop in pg:
            drop["node"] = "localhost"
            drop["island"] = "localhost"
        with tempfile.TemporaryDirectory() as tmp_dir:
            helm_client = HelmClient(
                deploy_dir=tmp_dir, deploy_name="dlg-test"
            )
            helm_client.create_helm_chart(json.dumps(pg), co_host=False)
            self.assertEqual(pg, json.loads(helm_client._physical_graph_file))
            self.assertEqual(1, helm_client._num_machines)

    def test_create_multi_node_helm_chart(self):
        pg = [
            {
                "oid": "A",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                "node": "localhost",
                "island": "localhost",
            },
            {
                "oid": "B",
                "categoryType": "Application",
                "dropclass": "dlg.apps.simple.SleepApp",
                "inputs": ["A"],
                "outputs": ["C"],
                "node": "localhost",
                "island": "localhost",
            },
            {
                "oid": "D",
                "categoryType": "Application",
                "dropclass": "dlg.apps.simple.SleepApp",
                "inputs": ["A"],
                "outputs": ["E"],
                "node": "127.0.0.2",
                "island": "127.0.0.2",
            },
            {
                "oid": "C",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                "node": "localhost",
                "island": "localhost",
            },
            {
                "oid": "E",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                "node": "127.0.0.2",
                "island": "127.0.0.2",
            },
        ]
        with tempfile.TemporaryDirectory() as tmp_dir:
            helm_client = HelmClient(
                deploy_dir=tmp_dir, deploy_name="dlg_test"
            )
            helm_client.create_helm_chart(json.dumps(pg), co_host=False)
            # TODO: Assert translation works
            self.assertEqual(2, helm_client._num_machines)

    @unittest.skip
    def test_submit_job(self):
        self.fail("Test not yet implemented")
