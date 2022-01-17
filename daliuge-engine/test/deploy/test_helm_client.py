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
import unittest
import tempfile
import os
import yaml

from dlg.common.version import version as dlg_version
from dlg.deploy.helm_client import HelmClient


class TestHelmClient(unittest.TestCase):

    def test_create_default_helm_chart(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            helm_client = HelmClient(deploy_dir=tmp_dir)
            helm_client.create_helm_chart([])
            print(helm_client._chart_name)
            chart_file_name = os.path.join(helm_client._chart_dir, "Chart.yaml")
            print(chart_file_name)
            with open(chart_file_name, 'r', encoding='utf-8') as chart_file:
                chart_data = yaml.safe_load(chart_file)
                self.assertEqual(helm_client._chart_name, chart_data['name'])
                self.assertEqual(dlg_version, chart_data['appVersion'])

    def test_create_single_node_helm_chart(self):
        self.fail("Test not yet implemented")

    def test_create_multi_node_helm_chart(self):
        self.fail("Test not yet implemented")

    def test_submit_job(self):
        self.fail("Test not yet implemented")
