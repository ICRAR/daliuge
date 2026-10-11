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

"""Tests for the typed algorithm-parameter interface on PartitionOptions."""

import pytest

from dlg.translator.stages.partition.algorithms.base import (
    MetisOptions,
    MinNumPartsOptions,
    MySarkarOptions,
    NoneOptions,
    PsoOptions,
)
from dlg.translator.stages.partition.stage import PartitionOptions


@pytest.mark.parametrize(
    ("options", "expected"),
    [
        (PartitionOptions(), MetisOptions()),
        (
            PartitionOptions(algo="mysarkar"),
            MySarkarOptions(),
        ),
        (
            PartitionOptions(algo="min_num_parts"),
            MinNumPartsOptions(),
        ),
        (PartitionOptions(algo="pso"), PsoOptions()),
        (PartitionOptions(algo="none"), NoneOptions()),
    ],
)
def test_partition_options_selects_the_matching_parameter_model(options, expected):
    """PartitionOptions exposes the typed model needed by its algorithm."""

    assert options.algorithm_parameters == expected


@pytest.mark.parametrize(
    ("options", "expected"),
    [
        (
            PartitionOptions(
                algo="metis",
                algo_params={"min_goal": 2, "ptype": 1, "max_load_imb": 75},
            ),
            MetisOptions(2, 1, 75),
        ),
        (
            PartitionOptions(
                algo="mysarkar",
                algo_params={"max_cpu": 16, "max_mem": 4096},
            ),
            MySarkarOptions(16, 4096),
        ),
        (
            PartitionOptions(
                algo="min_num_parts",
                algo_params={"deadline": 120, "max_cpu": 12, "time_greedy": 25},
            ),
            MinNumPartsOptions(120, 12, 25),
        ),
        (
            PartitionOptions(
                algo="pso",
                algo_params={
                    "max_cpu": 0,
                    "max_mem": 0,
                    "deadline": 45,
                    "topk": 0,
                    "swarm_size": 0,
                },
            ),
            PsoOptions(0, 0, 45, 0, 0),
        ),
    ],
)
def test_partition_options_passes_explicit_values_to_the_parameter_model(
    options, expected
):
    """The interface preserves explicit values, including zero."""

    assert options.algorithm_parameters == expected


@pytest.mark.parametrize(
    ("options", "expected"),
    [
        (
            PartitionOptions(
                algo="metis",
                algo_params={"min_goal": None, "ptype": None, "max_load_imb": None},
            ),
            MetisOptions(),
        ),
        (
            PartitionOptions(
                algo="pso",
                algo_params={"max_cpu": None, "max_mem": None, "topk": None},
            ),
            PsoOptions(),
        ),
    ],
)
def test_partition_options_preserves_none_fallback(options, expected):
    """None still means use the algorithm's established default."""

    assert options.algorithm_parameters == expected
