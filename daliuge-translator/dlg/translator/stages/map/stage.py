#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2020
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

import logging
from dataclasses import dataclass
from copy import deepcopy

from dlg.common.reproducibility.reproducibility import init_pg_repro_data
from dlg.translator.artefacts import PhysicalGraphTemplatePartitioned, PhysicalGraph

logger = logging.getLogger(f"dlg.{__name__}")


@dataclass(frozen=True)
class MapOptions:
    nodes: list[str]
    num_islands: int = 1
    co_host_dim: bool = True    # Not used by the CLI, keep for signature


class MapStage:
    """
    PGT-partitioned -> PG
    """
    name = 'map'

    def __init__(self, opts: MapOptions):
        self._opts = opts

    def run(self, pgtp: PhysicalGraphTemplatePartitioned) -> PhysicalGraph:
        return PhysicalGraph(
            drops=resource_map(
                pgt=deepcopy(pgtp.drops),
                nodes=self._opts.nodes,
                num_islands=self._opts.num_islands,
                co_host_dim=self._opts.co_host_dim
            ),
            reprodata=deepcopy(pgtp.reprodata)
        )

    def stamp(self, pg: PhysicalGraph) -> PhysicalGraph:
        return PhysicalGraph.from_wire(init_pg_repro_data(pg.to_wire()))


def resource_map(pgt, nodes, num_islands=1, co_host_dim=True):
    """Maps a Physical Graph Template `pgt` to `nodes`"""

    logger.info(
        "Resource mapping called with nodes: %s, islands: %s and co_host_dim: %s",
        nodes, num_islands, co_host_dim
    )
    if not nodes:
        err_info = "Empty node_list, cannot map the PG template"
        raise ValueError(err_info)

    # if co_host_dim == True the island nodes appear twice
    dim_list = nodes[0:num_islands]
    nm_list = nodes[num_islands:]
    if type(pgt[0]) is str:
        pgt = pgt[1]  # remove the graph name TODO: we may want to retain that
    for drop_spec in pgt:
        if "node" in drop_spec and "island" in drop_spec:
            nidx = int(drop_spec["node"][1:])  # skip '#'
            drop_spec["node"] = nm_list[nidx]
            iidx = int(drop_spec["island"][1:])  # skip '#'
            drop_spec["island"] = dim_list[iidx]

    return pgt  # now it's a PG
