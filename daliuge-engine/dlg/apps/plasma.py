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
import io
import os

import numpy as np
from casacore import tables
from dlg.drop import BarrierAppDROP
from dlg.meta import dlg_string_param, dlg_component, dlg_batch_input, \
    dlg_batch_output, dlg_streaming_input


class MSPlasmaReader(BarrierAppDROP):
    """
    A BarrierAppDROP that reads a CASA measurement from a plasma store and writes out to file.

    Example:
        a = FileDROP('a', 'a', filepath=in_file)
        b = MSPlasmaWriter('b', 'b')
        c = PlasmaDROP('c', 'c')
        d = MSPlasmaReader('d', 'd')
        e = FileDROP('e', 'e', filepath=out_file)
    """
    compontent_meta = dlg_component('MSPlasmaWriter', 'Measurement Set Plasma Writer.',
                                    [dlg_batch_input('binary/*', [])],
                                    [dlg_batch_output('binary/*', [])],
                                    [dlg_streaming_input('binary/*')])

    ms_output_path = dlg_string_param('ms_output_path', None)

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

        value = ms.pop('/')
        with tables.table(abs_path + '/' + filename, value[0], nrow=len(value[1])) as t:
            with t.row() as r:
                for idx, val in enumerate(value[1]):
                    r.put(idx, val)

        for key, value in ms.items():
            name = abs_path + '/' + filename + '/' + key
            with tables.table(name, value[0], nrow=len(value[1])) as t:
                with t.row() as r:
                    for idx, val in enumerate(value[1]):
                        if val.get('LOG', None) == []:
                            val['LOG'] = ''
                        if val.get('SCHEDULE', None) == []:
                            val['SCHEDULE'] = ''
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


class MSPlasmaWriter(BarrierAppDROP):
    """
    A BarrierAppDROP that reads a CASA measurement set and writes it out to a plasma store.

    Example:
        a = FileDROP('a', 'a', filepath=in_file)
        b = MSPlasmaWriter('b', 'b')
        c = PlasmaDROP('c', 'c')
        d = MSPlasmaReader('d', 'd')
        e = FileDROP('e', 'e', filepath=out_file)
    """
    compontent_meta = dlg_component('MSPlasmaWriter', 'Measurement Set Plasma Writer.',
                                    [dlg_batch_input('binary/*', [])],
                                    [dlg_batch_output('binary/*', [])],
                                    [dlg_streaming_input('binary/*')])

    ms_input_path = dlg_string_param('ms_input_path', None)

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
        self._read_table(path, ms, table_name='/')

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

