#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2022
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
A demo implementation of a Helm-based DAliuGE deployment.

Limitations:
- Assumes graphs will run on a single pod
- Does not support external graph components (yet)
"""
import argparse
import json
import os

from dlg.deploy.deployment_utils import find_node_ips
from dlg.dropmake import pg_generator
from dlg.deploy.helm_client import HelmClient


def get_pg(opts, node_managers: list, data_island_managers: list):
    if not opts.logical_graph and not opts.physical_graph:
        return []
    num_nms = len(node_managers)
    num_dims = len(data_island_managers)

    if opts.logical_graph:
        unrolled_graph = pg_generator.unroll(opts.logical_graph)
        pgt = pg_generator.partition(unrolled_graph, algo='mysarkar', num_partitons=num_nms,
                                     num_islands=num_dims)
        del unrolled_graph
    else:
        with open(opts.physical_graph, 'r', encoding='utf-8') as pg_file:
            pgt = json.load(pg_file)
    physical_graph = pg_generator.resource_map(pgt, node_managers + data_island_managers)
    # TODO: Add dumping to log-dir
    return physical_graph


def start_helm(physical_graph_template, num_nodes: int, deploy_dir: str):
    # TODO: Dynamic helm chart logging dir
    available_ips = find_node_ips()
    pgt = json.loads(physical_graph_template)
    pgt = pg_generator.partition(pgt, algo='min_num_parts', num_partitons=len(available_ips),
                                 num_islands=len(available_ips))
    pg = pg_generator.resource_map(pgt, available_ips + available_ips)
    helm_client = HelmClient(
        deploy_name='daliuge-daemon',
        chart_name='daliuge-daemon',
        deploy_dir=deploy_dir
    )
    try:
        helm_client.create_helm_chart(json.dumps(pg))
        helm_client.launch_helm()
        helm_client.submit_job()
        helm_client.teardown()
    except Exception as ex:
        raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-L',
        '--logical-graph',
        action="store",
        type=str,
        dest="logical_graph",
        help="The filename of the logical graph to deploy",
        default=None
    )
    parser.add_argument(
        "-P",
        "--physical_graph",
        action="store",
        type=str,
        dest="physical_graph",
        help="The filename of the physical graph (template) to deploy",
        default=None,
    )
    parser.add_argument(
        "-N",
        "--num_nodes",
        action="store",
        type=int,
        dest="num_nodes",
        help="The number of compute nodes you would like to try and deploy",
        default=1
    )

    options = parser.parse_args()
    if bool(options.logical_graph) == bool(options.physical_graph):
        parser.error(
            "Either a logical graph or physical graph filename must be specified"
        )
    for graph_file_name in (options.logical_graph, options.physical_graph):
        if graph_file_name and not os.path.exists(graph_file_name):
            parser.error(f"Cannot locate graph_file at {graph_file_name}")

    if options.num_nodes <= 0:
        parser.error("The number of nodes must be a positive integer")

    available_ips = find_node_ips()
    physical_graph = get_pg(options, available_ips, [available_ips[0]])
    # TODO: dynamic deployment directory.
    helm_client = HelmClient(
        deploy_name='daliuge-daemon',
        chart_name='daliuge-daemon',
        deploy_dir='/home/nicholas/dlg_temp/demo/',
    )
    helm_client.create_helm_chart(json.dumps(physical_graph))
    helm_client.launch_helm()
    # helm_client.submit_job()
    helm_client.teardown()


if __name__ == "__main__":
    main()
