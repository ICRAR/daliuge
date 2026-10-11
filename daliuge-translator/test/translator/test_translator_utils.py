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

import inspect
import unittest
from unittest.mock import patch

from dlg.common import path_utils
from dlg.translator.errors import GraphException
from dlg.translator.stages.partition.pgtp import MetisPGTP
from dlg.translator.web.translator_utils import (
    filter_dict_to_algo_params,
    prepare_lgt,
    unroll_and_partition_with_params,
)


class TestAlgorithmParameterFiltering(unittest.TestCase):
    def test_metis_filters_unrelated_legacy_parameters(self):
        params = {
            "max_load_imb": 100,
            "max_cpu": 8,
            "topk": 30,
        }

        self.assertEqual(
            filter_dict_to_algo_params(params, "metis"),
            {
                "max_load_imb": 100,
            },
        )

    def test_mysarkar_filters_unrelated_legacy_parameters(self):
        params = {
            "max_load_imb": 100,
            "max_cpu": 8,
            "max_mem": 512,
        }

        self.assertEqual(
            filter_dict_to_algo_params(params, "mysarkar"),
            {
                "max_cpu": 8,
                "max_mem": 512,
            },
        )

    def test_filter_without_algorithm_preserves_legacy_behaviour(self):
        params = {
            "max_load_imb": 100,
            "max_cpu": 8,
        }

        self.assertEqual(
            filter_dict_to_algo_params(params),
            params,
        )


class TestCompatibilityHelpers(unittest.TestCase):
    def test_prepare_lgt_signature_is_preserved(self):
        parameters = inspect.signature(
            prepare_lgt
        ).parameters

        self.assertEqual(
            list(parameters),
            [
                "filename",
                "rmode",
            ],
        )

    def test_unroll_and_partition_signature_is_preserved(self):
        parameters = inspect.signature(
            unroll_and_partition_with_params
        ).parameters

        self.assertEqual(
            list(parameters),
            [
                "lgt",
                "test",
                "algorithm",
                "num_partitions",
                "num_islands",
                "par_label",
                "algorithm_parameters",
            ],
        )
        self.assertEqual(
            parameters["algorithm"].default,
            "none",
        )
        self.assertEqual(
            parameters["num_partitions"].default,
            1,
        )
        self.assertEqual(
            parameters["num_islands"].default,
            0,
        )
        self.assertEqual(
            parameters["par_label"].default,
            "Partition",
        )
        self.assertIsNone(
            parameters["algorithm_parameters"].default
        )

    def test_prepare_lgt_keeps_legacy_dict_return_type(self):
        filename = path_utils.get_lg_fpath(
            "logical_graphs",
            "testLoop.graph",
        )

        result = prepare_lgt(
            filename,
            0,
        )

        self.assertIsInstance(
            result,
            dict,
        )

    def test_unroll_and_partition_keeps_legacy_pgt_return_type(self):
        filename = path_utils.get_lg_fpath(
            "logical_graphs",
            "testLoop.graph",
        )
        lgt = prepare_lgt(
            filename,
            0,
        )

        result = unroll_and_partition_with_params(
            lgt=lgt,
            test=False,
            algorithm="metis",
            num_partitions=1,
            num_islands=1,
        )

        self.assertIsInstance(
            result,
            MetisPGTP,
        )
        self.assertTrue(
            callable(result.to_pg_spec)
        )
        self.assertIsInstance(
            result.reprodata,
            dict,
        )

    def test_unroll_and_partition_raises_the_original_error(self):
        # REST catches GraphException and daliuge-engine calls this helper,
        # so the Pipeline's StageException wrapper must not leak out
        lgt = prepare_lgt(
            path_utils.get_lg_fpath(
                "logical_graphs",
                "testLoop.graph",
            ),
            0,
        )

        with patch(
            "dlg.translator.stages.unroll.stage.unroll",
            side_effect=GraphException("bad graph"),
        ):
            with self.assertRaises(GraphException) as ctx:
                unroll_and_partition_with_params(
                    lgt=lgt,
                    test=False,
                )

        self.assertEqual(str(ctx.exception), "bad graph")


if __name__ == "__main__":
    unittest.main()
