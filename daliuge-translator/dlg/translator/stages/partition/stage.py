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
from dataclasses import dataclass, field
from copy import deepcopy

from dlg.translator.errors import GraphException
from dlg.translator.artefacts import PhysicalGraphTemplate, PhysicalGraphTemplatePartitioned
from dlg.common.reproducibility.reproducibility import init_pgt_partition_repro_data
from dlg.translator.stages.partition.algorithms.base import AlgorithmOptions
from dlg.translator.stages.partition.algorithms.registry import (
    algorithm_code,
    algorithm_name,
    build_options,
    get_algorithm,
    known_algorithms as registry_known_algorithms,
    option_names,
)

logger = logging.getLogger(f"dlg.{__name__}")


@dataclass(frozen=True)
class PartitionOptions:
    algo: str = "metis"
    num_partitions: int = 1
    num_islands: int = 1
    partition_label: str = "partition"
    algo_params: dict = field(default_factory=dict)

    @property
    def algorithm_parameters(self) -> AlgorithmOptions:
        """Return the typed options selected by this partition option set."""
        return build_options(self.algo, self.algo_params)


class PartitionStage:
    """
    PGT -> PGT-partitioned
    """
    name = "partition"

    def __init__(self, opts: PartitionOptions = PartitionOptions()):
        self._opts = opts

    def run(self, pgt: PhysicalGraphTemplate) -> PhysicalGraphTemplatePartitioned:
        return PhysicalGraphTemplatePartitioned(
            drops=partition(
                pgt=deepcopy(pgt.drops),
                algo=self._opts.algo,
                num_partitions=self._opts.num_partitions,
                num_islands=self._opts.num_islands,
                partition_label=self._opts.partition_label,
                strict=True,
                **self._opts.algo_params
            ),
            reprodata=deepcopy(pgt.reprodata)
        )

    def stamp(self, pgtp: PhysicalGraphTemplatePartitioned) -> PhysicalGraphTemplatePartitioned:
        return PhysicalGraphTemplatePartitioned.from_wire(
            init_pgt_partition_repro_data(pgtp.to_wire()))


def partition(
    pgt,
    algo,
    num_partitions=1,
    num_islands=1,
    partition_label="partition",
    show_gojs=False,
    *,
    strict=False,
    **algo_params,
):
    """Partitions a Physical Graph Template"""

    if isinstance(algo, str):
        try:
            algo = algorithm_code(algo)
        except KeyError as exc:
            raise ValueError(
                "Unknown partitioning algorithm: %s. Known algorithms are: %r"
                % (algo, registry_known_algorithms())
            ) from exc

    try:
        resolved_algo_name = algorithm_name(algo)
    except KeyError as exc:
        raise GraphException(
            "Unknown partition algorithm: %d. Known algorithms are: %r"
            % (algo, registry_known_algorithms())
        ) from exc

    logger.info(
        "Running partitioning with algorithm=%s, %d partitions, "
        "%d islands, and parameters=%r",
        resolved_algo_name,
        num_partitions,
        num_islands,
        algo_params,
    )

    algorithm = get_algorithm(algo)
    if not strict:
        algo_params = _drop_unused_params(resolved_algo_name, algo_params)
    options = build_options(algo, algo_params)

    pgt = algorithm.partition(
        pgt,
        num_partitions=num_partitions,
        partition_label=partition_label,
        options=options,
    )

    pgt.to_gojs_json(string_rep=False, visual=show_gojs)
    if not show_gojs:
        pgt = pgt.to_pg_template(
            num_partitions + num_islands,
            num_islands=num_islands,
        )
    return pgt


def _drop_unused_params(algo_name, algo_params):
    """
    Keep only the parameters the algorithm consumes, warning about the rest.

    Explicit None values mean "use the default", so they are dropped silently.
    """
    allowed = option_names(algo_name)
    ignored = sorted(
        name
        for name, value in algo_params.items()
        if name not in allowed and value is not None
    )
    if ignored:
        logger.warning(
            "Ignoring parameters not used by partition algorithm %s: %s",
            algo_name,
            ", ".join(ignored),
        )
    return {
        name: value
        for name, value in algo_params.items()
        if name in allowed
    }


def known_algorithms():
    return registry_known_algorithms()
