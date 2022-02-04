#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2016
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
Contains a module translating physical graphs to kubernetes helm charts.
"""
import json
import re
import time
import os
import sys
import shutil
import pathlib

import dlg
import yaml
import subprocess
from dlg.common.version import version as dlg_version
from dlg.restutils import RestClient
from dlg.deploy.common import submit


def _write_chart(chart_dir, name: str, chart_name: str, version: str, app_version: str, home: str,
                 description, keywords: list, sources: list, kubeVersion: str):
    chart_info = {'apiVersion': "v2", 'name': chart_name, 'type': 'application', 'version': version,
                  'appVersion': app_version, 'home': home, 'description': description,
                  'keywords': keywords, 'sources': sources, 'kubeVersion': kubeVersion}
    # TODO: Fix app_version quotations.
    with open(f'{chart_dir}{os.sep}{name}', 'w', encoding='utf-8') as chart_file:
        yaml.dump(chart_info, chart_file)


def _write_values(chart_dir, config):
    with open(f"{chart_dir}{os.sep}custom-values.yaml", 'w', encoding='utf-8') as value_file:
        yaml.dump(config, value_file)


def _read_values(chart_dir):
    with open(f"{chart_dir}{os.sep}values.yaml", 'r', encoding='utf-8') as old_file:
        data = yaml.safe_load(old_file)
    with open(f"{chart_dir}{os.sep}values.yaml", 'r', encoding='utf-8') as custom_file:
        new_data = yaml.safe_load(custom_file)
    data.update(new_data)
    return data


def _find_resources(pgt_data):
    pgt = json.loads(pgt_data)
    nodes = list(map(lambda x: x['node'], pgt))
    islands = list(map(lambda x: x['island'], pgt))
    num_islands = len(dict(zip(islands, nodes)))
    num_nodes = len(nodes)
    return num_islands, num_nodes


class HelmClient:
    """
    Writes necessary files to launch job with kubernetes.
    """

    def __init__(self, deploy_name, chart_name="daliuge-daemon", deploy_dir="./",
                 submit=True, chart_version="0.1.0",
                 value_config=None, physical_graph_file=None, chart_vars=None):
        if value_config is None:
            value_config = dict()
        self._chart_name = chart_name
        self._chart_vars = {'name': 'daliuge-daemon',
                            'appVersion': 'v1.0.0',
                            'home': 'https://github.com/ICRAR/daliuge/daliuge-k8s',
                            'description': 'DALiuGE k8s deployment',
                            'keywords': ['daliuge', 'workflow'],
                            'sources': ['https://github.com/ICRAR/daliuge/daliuge-k8s'],
                            'kubeVersion': ">=1.10.0-0"
                            }
        if chart_vars is not None:
            self._chart_vars.update(chart_vars)
        self._deploy_dir = deploy_dir
        self._chart_dir = os.path.join(self._deploy_dir, 'daliuge-daemon')
        self._chart_version = chart_version
        self._deploy_name = deploy_name
        self._submit = submit
        self._value_data = value_config if value_config is not None else {}
        self._submission_endpoint = None
        if physical_graph_file is not None:
            self._set_physical_graph(physical_graph_file)

        # Copy in template files.
        library_root = pathlib.Path(os.path.dirname(dlg.__file__)).parent.parent
        print(library_root)
        if sys.version_info >= (3, 8):
            shutil.copytree(os.path.join(library_root, 'daliuge-k8s', 'helm'), self._deploy_dir,
                            dirs_exist_ok=True)
        else:
            shutil.copytree(os.path.join(library_root, 'daliuge-k8s', 'helm'), self._deploy_dir)

    def _set_physical_graph(self, physical_graph_content):
        self._physical_graph_file = physical_graph_content
        self._num_islands, self._num_nodes = _find_resources(
            self._physical_graph_file)

    def create_helm_chart(self, physical_graph_content):
        """
        Translates a physical graph to a kubernetes helm chart.
        For now, it will just try to run everything in a single container.
        """
        _write_chart(self._chart_dir, 'Chart.yaml', self._chart_name, self._chart_version,
                     dlg_version,
                     self._chart_vars['home'], self._chart_vars['description'],
                     self._chart_vars['keywords'], self._chart_vars['sources'],
                     self._chart_vars['kubeVersion'])
        # Update values.yaml
        _write_values(self._chart_dir, self._value_data)
        self._value_data = _read_values(self._chart_dir)
        # Add charts
        # TODO: Add charts to helm
        self._set_physical_graph(physical_graph_content)
        # Update template
        # TODO: Update templates in helm

    def launch_helm(self):
        """
        Launches the built helm chart using the most straightforward commands possible.
        Assumes all files are prepared and validated.
        """
        if self._submit:
            os.chdir(self._deploy_dir)
            instruction = f'helm install {self._deploy_name} {self._chart_name}/  ' \
                          f'--values {self._chart_name}{os.sep}custom-values.yaml'
            print(subprocess.check_output([instruction],
                                          shell=True).decode('utf-8'))
            query = str(subprocess.check_output(['kubectl get svc -o wide'], shell=True))
            # WARNING: May be problematic later if multiple services are running
            pattern = r"-service\s*ClusterIP\s*\d+\.\d+\.\d+\.\d+"
            ip_pattern = r"\d+\.\d+\.\d+\.\d+"
            outcome = re.search(pattern, query)
            if outcome:
                manager_ip = re.search(ip_pattern, outcome.string)
                self._submission_endpoint = manager_ip.group(0)
                client = RestClient(self._submission_endpoint,
                                    self._value_data['service']['daemon']['port'])
                data = json.dumps({'nodes': ["127.0.0.1"]}).encode('utf-8')
                time.sleep(5)  # TODO: Deterministic deployment information
                client._POST('/managers/island/start', content=data,
                             content_type='application/json')
                client._POST('/managers/master/start', content=data,
                             content_type='application/json')
            else:
                print("Could not find manager IP address")

        else:
            print(f"Created helm chart {self._chart_name} in {self._deploy_dir}")

    def teardown(self):
        subprocess.check_output(['helm uninstall daliuge-daemon'], shell=True)

    def submit_job(self):
        """
        There is a semi-dynamic element to fetching the IPs of Node(s) to deploy to.
        Hence, launching the chart and initiating graph execution have been de-coupled.
        """
        pg_data = json.loads(self._physical_graph_file)
        submit(pg_data, self._submission_endpoint)
