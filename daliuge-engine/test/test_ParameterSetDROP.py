#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2014
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
from dlg.data.drops.parset_drop import ParameterSetDROP
from dlg.droputils import allDropContents


class test_ParameterSetDROP(unittest.TestCase):
    kwargs = {
        "mode": None,
        "applicationArgs": {
            "Cimager": {
                "value": 2,
                "defaultValue": 2,
                "type": "int",
                "description": "",
            },
            "StringParam": {
                "value": "param",
                "defaultValue": "param",
                "type": "string",
                "description": "",
            },
            "Boolparam": {
                "value": True,
                "defaultValue": False,
                "type": "boolean",
                "description": "",
            },
        },
        "config_data": "",
        "iid": -1,
        "rank": 0,
        "consumers": ["drop-1", "drop-2"],
    }

    def test_initialize(self):
        yanda_kwargs = dict(self.kwargs)
        yanda_kwargs["mode"] = "YANDA"

        yanda_parset = ParameterSetDROP(
            oid="a",
            uid="a",
            **yanda_kwargs,
        )

        yanda_kwargs["mode"] = "json"
        json_parset = ParameterSetDROP(oid="b", uid="b", **yanda_kwargs)

        yanda_output = "Cimager=2\nStringParam=param\nBoolparam=True"
        json_output = '{"Cimager": {"value": 2, "description": "", "type": "int", "default": 2}, "StringParam": {"value": "param", "description": "", "type": "string", "default": "param"}, "Boolparam": {"value": true, "description": "", "type": "boolean", "default": false}}'

        yanda_parset.setCompleted()
        json_parset.setCompleted()

        yanda_serialised = allDropContents(yanda_parset).decode("utf-8")
        self.assertEqual(yanda_output, yanda_serialised)

        json_serialised = allDropContents(json_parset).decode("utf-8")
        self.assertEqual(json_output, json_serialised)
