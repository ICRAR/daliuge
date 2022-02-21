#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2015
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
import os
import io
import numpy as np
import logging
import asyncio

from dlg.drop import BarrierAppDROP, AppDROP
from dlg.meta import dlg_string_param
from dlg.ddap_protocol import AppDROPStates
from ..meta import dlg_component, dlg_batch_input
from ..meta import dlg_batch_output, dlg_streaming_input

from threading import Thread
from multiprocessing import Lock
from casacore import tables

from cbf_sdp.consumers import plasma_writer
from cbf_sdp import plasma_processor
from cbf_sdp import utils, icd, msutils

logger = logging.getLogger(__name__)


##
# @brief MSStreamingPlasmaConsumer
# @details Stream Measurement Set one correlator timestep at a time
# via Plasma.
# @par EAGLE_START
# @param category PythonApp
# @param tag daliuge
# @param[in] aparam/plasma_path Plasma Path//String/readwrite/False/
#     \~English Path to plasma store.
# @param[in] cparam/appclass Application class/dlg.apps.plasma.MSStreamingPlasmaConsumer/String/readonly/False/
#     \~English Application class
# @param[in] cparam/execution_time Execution Time/5/Float/readonly/False/
#     \~English Estimated execution time
# @param[in] cparam/num_cpus No. of CPUs/1/Integer/readonly/False/
#     \~English Number of cores used
# @param[in] cparam/group_start Group start/False/Boolean/readwrite/False/
#     \~English Is this node the start of a group?
# @param[in] cparam/input_error_threshold "Input error rate (%)"/0/Integer/readwrite/False/
#     \~English the allowed failure rate of the inputs (in percent), before this component goes to ERROR state and is not executed
# @param[in] cparam/n_tries Number of tries/1/Integer/readwrite/False/
#     \~English Specifies the number of times the 'run' method will be executed before finally giving up
# @param[in] port/plasma_ms_input Plasma MS Input/Measurement Set/
#     \~English Plasma MS input
# @param[out] port/output_file Output File/File/
#     \~English MS output file
# @par EAGLE_END
class MSStreamingPlasmaConsumer(AppDROP):
    component_meta = dlg_component(
        "MSStreamingPlasmaConsumer",
        "MS Plasma Consumer",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    plasma_path = dlg_string_param("plasma_path", "/tmp/plasma")

    def initialize(self, **kwargs):
        self.config = {
            "reception": {
                "consumer": "plasma_writer",
                "test_entry": 5,
                "plasma_path": self.plasma_path,
            }
        }
        self.thread = None
        self.lock = Lock()
        self.started = False
        self.complete_called = 0
        super(MSStreamingPlasmaConsumer, self).initialize(**kwargs)

    async def _run_consume(self):
        outs = self.outputs
        if len(outs) < 1:
            raise Exception(
                "At least one output MS should have been connected to %r" % self
            )
        self.output_file = outs[0]._path
        if self.plasma_path:
            self.config["reception"]["plasma_path"] = self.plasma_path

        runner = plasma_processor.Runner(
            self.output_file,
            self.config["reception"]["plasma_path"],
            max_payload_misses=30,
            max_measurement_sets=1,
        )
        runner.process_timeout = 0.1
        await runner.run()

    def dataWritten(self, uid, data):
        with self.lock:
            if self.started is False:

                def thread_func():
                    loop = asyncio.new_event_loop()
                    loop.run_until_complete(self._run_consume())

                self.thread = Thread(target=thread_func)
                self.thread.start()
                self.started = True

                logger.info("MSStreamingPlasmaConsumer in RUNNING State")
                self.execStatus = AppDROPStates.RUNNING

    def dropCompleted(self, uid, drop_state):
        n_inputs = len(self.streamingInputs)
        with self.lock:
            self.complete_called += 1
            move_to_finished = self.complete_called == n_inputs

        if move_to_finished:
            logger.info("MSStreamingPlasmaConsumer in FINISHED State")
            self.execStatus = AppDROPStates.FINISHED
            self._notifyAppIsFinished()
            self.thread.join()


##
# @brief MSStreamingPlasmaProducer
# @details Stream Measurement Set one correlator timestep at a time
# via Plasma.
# @par EAGLE_START
# @param category PythonApp
# @param tag daliuge
# @param[in] aparam/plasma_path Plasma Path//String/readwrite/False/
#     \~English Path to plasma store
# @param[in] cparam/appclass Application class/dlg.apps.plasma.MSStreamingPlasmaProducer/String/readonly/False/
#     \~English Application class
# @param[in] cparam/execution_time Execution Time/5/Float/readonly/False/
#     \~English Estimated execution time
# @param[in] cparam/num_cpus No. of CPUs/1/Integer/readonly/False/
#     \~English Number of cores used
# @param[in] cparam/group_start Group start/False/Boolean/readwrite/False/
#     \~English Is this node the start of a group?
# @param[in] cparam/input_error_threshold "Input error rate (%)"/0/Integer/readwrite/False/
#     \~English the allowed failure rate of the inputs (in percent), before this component goes to ERROR state and is not executed
# @param[in] cparam/n_tries Number of tries/1/Integer/readwrite/False/
#     \~English Specifies the number of times the 'run' method will be executed before finally giving up
# @param[in] port/input_file Input File/File/
#     \~English MS input file
# @param[out] port/plasma_ms_output Plasma MS Output/Measurement Set/
#     \~English Plasma MS output
# @par EAGLE_END
class MSStreamingPlasmaProducer(BarrierAppDROP):
    component_meta = dlg_component(
        "MSStreamingPlasmaProducer",
        "MS Plasma Producer",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    plasma_path = dlg_string_param("plasma_path", "/tmp/plasma")

    def initialize(self, **kwargs):
        super(MSStreamingPlasmaProducer, self).initialize(**kwargs)
        self.config = {
            "reception": {
                "consumer": "plasma_writer",
                "test_entry": 5,
                "plasma_path": self.plasma_path,
            }
        }

    async def _run_producer(self):
        if self.plasma_path:
            self.config["reception"]["plasma_path"] = self.plasma_path

        c = plasma_writer.consumer(self.config, utils.FakeTM(self.input_file))
        while not c.find_processors():
            await asyncio.sleep(0.1)

        async for vis, ts, ts_fraction in msutils.vis_reader(self.input_file):
            payload = icd.Payload()
            payload.timestamp_count = ts
            payload.timestamp_fraction = ts_fraction
            payload.channel_count = len(vis)
            payload.visibilities = vis
            await c.consume(payload)
            # await asyncio.sleep(0.01)

            # For for the response to arrive
            await asyncio.get_event_loop().run_in_executor(
                None, c.get_response, c.output_refs.pop(0), 10
            )

    def run(self):
        # self.input_file = kwargs.get('input_file')
        ins = self.inputs
        if len(ins) < 1:
            raise Exception("At least one MS should have been connected to %r" % self)
        self.input_file = ins[0]._path
        self.outputs[0].write(b"init")
        loop = asyncio.new_event_loop()
        loop.run_until_complete(self._run_producer())


##
# @brief MSPlasmaReader
# @details Batch read entire Measurement Set from Plasma.
# @par EAGLE_START
# @param category PythonApp
# @param tag daliuge
# @param[in] cparam/appclass Application class/dlg.apps.plasma.MSPlasmaReader/String/readonly/False/
#     \~English Application class
# @param[in] cparam/execution_time Execution Time/5/Float/readonly/False/
#     \~English Estimated execution time
# @param[in] cparam/num_cpus No. of CPUs/1/Integer/readonly/False/
#     \~English Number of cores used
# @param[in] cparam/group_start Group start/False/Boolean/readwrite/False/
#     \~English Is this node the start of a group?
# @param[in] cparam/input_error_threshold "Input error rate (%)"/0/Integer/readwrite/False/
#     \~English the allowed failure rate of the inputs (in percent), before this component goes to ERROR state and is not executed
# @param[in] cparam/n_tries Number of tries/1/Integer/readwrite/False/
#     \~English Specifies the number of times the 'run' method will be executed before finally giving up
# @param[in] port/plasma_ms_input Plasma MS Input/Measurement Set/
#     \~English Plasma MS store input
# @param[out] port/output_ms Output MS/Measurement Set/
#     \~English Output MS file
# @par EAGLE_END
class MSPlasmaReader(BarrierAppDROP):
    def initialize(self, **kwargs):
        super(MSPlasmaReader, self).initialize(**kwargs)

    def _write_table(self, ms, path, delete=True):
        if delete is True:
            try:
                os.rmdir(path)
            except OSError:
                pass

        abs_path = os.path.dirname(os.path.abspath(path))
        filename = os.path.basename(path)

        value = ms.pop("/")
        with tables.table(abs_path + "/" + filename, value[0], nrow=len(value[1])) as t:
            with t.row() as r:
                for idx, val in enumerate(value[1]):
                    r.put(idx, val)

        for key, value in ms.items():
            name = abs_path + "/" + filename + "/" + key
            with tables.table(name, value[0], nrow=len(value[1])) as t:
                with t.row() as r:
                    for idx, val in enumerate(value[1]):
                        if val.get("LOG", None) == []:
                            val["LOG"] = ""
                        if val.get("SCHEDULE", None) == []:
                            val["SCHEDULE"] = ""
                        r.put(idx, val)

    def _deserialize_table(self, in_stream, path):
        load_bytes = io.BytesIO(in_stream)
        ms = np.load(load_bytes, allow_pickle=True).flat[0]
        self._write_table(ms, path)

    def run(self, **kwargs):
        if len(self.inputs) != 1:
            raise Exception("This application read only from one DROP")
        if len(self.outputs) != 1:
            raise Exception("This application writes only one DROP")

        inp = self.inputs[0]
        out = self.outputs[0].path

        desc = inp.open()
        input_stream = inp.read(desc)
        self._deserialize_table(input_stream, out)


##
# @brief MSPlasmaWriter
# @details Batch write entire Measurement Set to Plasma.
# @par EAGLE_START
# @param category PythonApp
# @param tag daliuge
# @param[in] cparam/appclass Application class/dlg.apps.plasma.MSPlasmaWriter/String/readonly/False/
#     \~English Application class
# @param[in] cparam/execution_time Execution Time/5/Float/readonly/False/
#     \~English Estimated execution time
# @param[in] cparam/num_cpus No. of CPUs/1/Integer/readonly/False/
#     \~English Number of cores used
# @param[in] cparam/group_start Group start/False/Boolean/readwrite/False/
#     \~English Is this node the start of a group?
# @param[in] cparam/input_error_threshold "Input error rate (%)"/0/Integer/readwrite/False/
#     \~English the allowed failure rate of the inputs (in percent), before this component goes to ERROR state and is not executed
# @param[in] cparam/n_tries Number of tries/1/Integer/readwrite/False/
#     \~English Specifies the number of times the 'run' method will be executed before finally giving up
# @param[in] port/input_ms Input MS/Measurement Set/
#     \~English Input MS file
# @param[out] port/plasma_ms_output Plasma MS Output/Measurement Set/
#     \~English Plasma MS store output
# @par EAGLE_END
class MSPlasmaWriter(BarrierAppDROP):
    def initialize(self, **kwargs):
        super(MSPlasmaWriter, self).initialize(**kwargs)

    def _read_table(self, table_path, ms, table_name=None):
        if not table_name:
            table_name = os.path.basename(table_path)

        ms[table_name] = []
        with tables.table(table_path) as t:
            ms[table_name].append(t.getdesc())
            ms[table_name].append([])
            for row in t:
                ms[table_name][1].append(row)

    def _serialize_table(self, path):
        ms = {}
        self._read_table(path, ms, table_name="/")

        with tables.table(path) as t:
            sub = t.getsubtables()
            for i in sub:
                self._read_table(i, ms)

        out_stream = io.BytesIO()
        np.save(out_stream, ms, allow_pickle=True)
        return out_stream.getvalue()

    def run(self, **kwargs):
        if len(self.inputs) != 1:
            raise Exception("This application read only from one DROP")
        if len(self.outputs) != 1:
            raise Exception("This application writes only one DROP")

        inp = self.inputs[0].path
        out = self.outputs[0]
        out_bytes = self._serialize_table(inp)
        out.write(out_bytes)
