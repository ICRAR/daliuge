#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2017
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

import ctypes
import functools

import six

from ..drop import BarrierAppDROP
from ..exceptions import InvalidDropException


_read_cb_type = ctypes.CFUNCTYPE(ctypes.c_size_t,
                                 ctypes.POINTER(ctypes.c_char),
                                 ctypes.c_size_t)

_write_cb_type = ctypes.CFUNCTYPE(ctypes.c_size_t,
                                  ctypes.POINTER(ctypes.c_char),
                                  ctypes.c_size_t)

class CDlgInputInfo(ctypes.Structure):
    _fields_ = [('uid', ctypes.c_char_p),
                ('oid', ctypes.c_char_p),
                ('read', _read_cb_type)]

class CDlgOutputInfo(ctypes.Structure):
    _fields_ = [('uid', ctypes.c_char_p),
                ('oid', ctypes.c_char_p),
                ('write', _write_cb_type)]

class CDlgAppInfo(ctypes.Structure):
    _fields_ = [('uid', ctypes.c_char_p),
                ('oid', ctypes.c_char_p),
                ('inputs', ctypes.POINTER(CDlgInputInfo)),
                ('n_inputs', ctypes.c_uint),
                ('outputs', ctypes.POINTER(CDlgOutputInfo)),
                ('n_outputs', ctypes.c_uint),
                ('data', ctypes.c_void_p),]

class DynlibApp(BarrierAppDROP):

    def initialize(self, **kwargs):
        super(DynlibApp, self).initialize(**kwargs)

        if 'lib' not in kwargs:
            raise InvalidDropException("library not specified")

        # Try with a simple name, or as full path
        from ctypes.util import find_library
        libname = kwargs.pop('lib')
        lib = find_library(libname) or libname

        self.lib = ctypes.cdll.LoadLibrary(lib)
        expected_functions = ('init_app_drop', 'run')
        for fname in expected_functions:
            if not hasattr(self.lib, fname):
                raise InvalidDropException("%s doesn't have function %s" % (lib, fname))

        # Create the initial contents of the dlg_app_info structure
        self._uid_b = six.b(self.uid)
        self._oid_b = six.b(self.oid)
        self._c_app_info = CDlgAppInfo(self._uid_b, self._oid_b, None, 0, None, 0, None)

        # Let the shared library initialize this app
        if self.lib.init_app_drop(ctypes.pointer(self._c_app_info)):
            raise RuntimeError("Application could not be initialized")

    def run(self):

        # read / write callbacks
        def _read(input_read, desc, buf, n):
            x = input_read(desc, n)
            ctypes.memmove(ctypes.addressof(buf.contents), x, len(x))
            return len(x)

        def _write(output_write, buf, n):
            return output_write(buf[:n])

        # Update our dlg_app_info structure to include input/output information
        input_infos = []
        opened_info = []
        for uid, i in self._inputs.items():
            desc = i.open()
            opened_info.append((i, desc))
            r = _read_cb_type(functools.partial(_read, i.read, desc))
            input_infos.append(CDlgInputInfo(six.b(i.uid), six.b(i.oid), r))
        self._c_app_info.inputs = (CDlgInputInfo * len(input_infos))(*input_infos)
        self._c_app_info.n_inputs = len(input_infos)

        output_infos = []
        for uid, o in self._outputs.items():
            w = _write_cb_type(functools.partial(_write, self._outputs[uid].write))
            output_infos.append(CDlgOutputInfo(six.b(o.uid), six.b(o.oid), w))
        self._c_app_info.outputs = (CDlgOutputInfo * len(output_infos))(*output_infos)
        self._c_app_info.n_outputs = len(output_infos)

        try:
            self.lib.run(ctypes.pointer(self._c_app_info))
        finally:
            for x, desc  in opened_info:
                x.close(desc);