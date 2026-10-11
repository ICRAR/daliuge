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

import unittest

from dlg.translator.web.translator_utils import filter_dict_to_algo_params


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


if __name__ == "__main__":
    unittest.main()
