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
from collections import defaultdict
from types import SimpleNamespace
from unittest.mock import Mock, patch

import dlg.translator.stages.unroll.instantiate as instantiate_module
from dlg.translator.vocabulary import Categories


class TestServiceHandlerInstantiationRouting(unittest.TestCase):

    def test_group_service_routes_through_service_handler(self):
        service = SimpleNamespace(
            id="service",
            category=Categories.SERVICE,
            is_group=True,
            jd={"category": Categories.SERVICE},
            group_keys=None,
            dop=1,
            children=[],
        )

        drops = defaultdict(list)
        graph = SimpleNamespace(
            _session_id="session",
            _done_dict={"service": service},
            _drop_dict=drops,
        )

        handler = Mock()
        handler.instantiate.return_value = [{"oid": "service-drop"}]

        with patch.object(
            instantiate_module,
            "get_handler_for_node",
            return_value=handler,
        ):
            instantiate_module.lgn_to_pgn(graph, service)

        handler.instantiate.assert_called_once()
        self.assertEqual(
            [{"oid": "service-drop"}],
            drops["service"],
        )


if __name__ == "__main__":
    unittest.main()
