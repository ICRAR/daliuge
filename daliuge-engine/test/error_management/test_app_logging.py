#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2025
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
import unittest
import base64
import pickle

from dlg.apps import pyfunc
from dlg.droputils import DROPWaiterCtx
from dlg.data.drops import InMemoryDROP

from test.dlg_engine_testutils import AppArgsStore

# func_code = (
#     """
#     def multiplier(x, y):
#         return x*y
#     """
# )

def multiplier(x, y):
    return x*y

def multiplier_error(x,y):
    return x["test"]

translate = lambda x: base64.b64encode(pickle.dumps(x))

def filter_logs_by_level(logs: list[dict], level: str):
    return [record for record in logs if record["Level"]==level]

class TestAppLogStorage(unittest.TestCase):
    """
    Test concerning itself with the dlg.apps.app_base.InstanceLogHandler
    """
    
    def setUp(self):
        """
        Construct a PyFunc AppDrop within the base logging level
        :return:
        """
        self.name = f"dlg.{__name__}"
        self.logger = logging.getLogger(self.name)
        self.logger.root.setLevel("WARNING")

        xDrop = InMemoryDROP("xDrop", "xDrop", pydata=translate(4))
        yDrop = InMemoryDROP("yDrop", "yDrop", pydata=translate(2))
        self.result = InMemoryDROP("result", "result")
        self.input_drops = [xDrop, yDrop]
        func_name = "test.error_management.test_app_logging.multiplier_error"
        fcode, fdefaults = pyfunc.serialize_func(func_name)

        application_args = AppArgsStore()
        application_args.add_args(name="x",usage="InputPort")
        application_args.add_args(name="y",usage="InputPort")
        application_args.add_args(name="result", usage="OutputPort")

        self.app = pyfunc.PyFuncApp(
                              "PyFuncTest",
                              "PyFuncTest",
                              func_name=func_name,
                              func_code=fcode,
                              func_defaults=fdefaults,
                              kwargs=application_args)

        for drop in self.input_drops:
            self.app.addInput(drop)
        self.app.addOutput(self.result)

    def tearDown(self):
        self.logger.root.setLevel("WARNING")


    def test_default_logging(self):
        """
        Test that logs that are tracked  as expected when we are in the default logging
        level.

        Default level - WARNING
        This means we would expect to see only Warnings or Errors, and nothing below (e.g.
        INFO or DEBUG).
        """
        logger = logging.getLogger(self.name)
        name = logging.getLevelName(logger.root.getEffectiveLevel())
        self.assertEqual("WARNING", name)
        with DROPWaiterCtx(self, self.result, 5):
            for drop in self.input_drops:
                drop.setCompleted()
        logs = filter_logs_by_level(self.app.getLogs(), "USER")
        self.assertEqual(0, len(logs))
        logs = filter_logs_by_level(self.app.getLogs(), "DEBUG")
        self.assertEqual(0, len(logs))
        logs = filter_logs_by_level(self.app.getLogs(), "ERROR")
        self.assertLess(0, len(logs))

    def test_runtime_debug_logging(self):
        """
        Test that more logs are generated we are in the debug logging level.
        """
        logger = logging.getLogger(self.name)
        logger.root.setLevel("DEBUG")
        name = logging.getLevelName(logger.root.getEffectiveLevel())
        self.assertEqual("DEBUG", name)
        with DROPWaiterCtx(self, self.result, 5):
            for drop in self.input_drops:
                drop.setCompleted()

        logs = filter_logs_by_level(self.app.getLogs(), "USER")
        self.assertLess(0, len(logs))
        logs = filter_logs_by_level(self.app.getLogs(), "DEBUG")
        self.assertLess(0, len(logs))
        logs = filter_logs_by_level(self.app.getLogs(), "ERROR")
        self.assertLess(0, len(logs))