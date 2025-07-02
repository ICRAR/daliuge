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
import logging
import tempfile

import dlg.exceptions
import dlg.restutils
from dlg.deploy.helm_client import HelmClient
from dlg.dropmake import pg_generator


def get_pg(opts, num_node_managers, num_data_island_managers):
    if not opts.logical_graph and not opts.physical_graph:
        return []

    if opts.logical_graph:
        unrolled_graph = pg_generator.unroll(opts.logical_graph)
        pgt = pg_generator.partition(
            unrolled_graph,
            algo="metis",
            num_partitions=num_node_managers,
            num_islands=num_data_island_managers,
        )
        del unrolled_graph
    else:
        with open(opts.physical_graph, "r", encoding="utf-8") as pg_file:
            pgt = json.load(pg_file)
    return pgt


def start_helm(physical_graph_template, num_nodes: int, deploy_dir: str):
    # TODO: Dynamic helm chart logging dir
    pgt = json.loads(physical_graph_template)
    pgt = pg_generator.partition(pgt, algo="metis", num_partitons=1, num_islands=1)
    helm_client = HelmClient(
        deploy_name="daliuge-daemon", chart_name="daliuge-daemon", deploy_dir=deploy_dir
    )
    helm_client.create_helm_chart(json.dumps(pgt), co_host=True)
    try:
        helm_client.launch_helm()
        helm_client.submit_pgt()
    except dlg.restutils.RestClientException as exp:
        raise exp
    except dlg.exceptions.InvalidGraphException as exp2:
        raise exp2
    finally:
        helm_client.teardown()
        logging.info("Finished deployment on %d", num_nodes)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-L",
        "--logical-graph",
        action="store",
        type=str,
        dest="logical_graph",
        help="The filename of the logical graph to deploy",
        default=None,
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
        default=1,
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

    physical_graph = get_pg(options, options.num_nodes, 1)
    # TODO: dynamic deployment directory.
    with tempfile.TemporaryDirectory() as tdir:
        helm_client = HelmClient(
            deploy_name="daliuge-daemon",
            chart_name="daliuge-daemon",
            deploy_dir=tdir,
        )
        helm_client.create_helm_chart(json.dumps(physical_graph))
        try:
            helm_client.launch_helm()
            helm_client.submit_and_monitor_pgt()
        except dlg.restutils.RestClientException as exp:
            raise exp
        except dlg.exceptions.InvalidGraphException as exp2:
            raise exp2
        finally:
            helm_client.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
