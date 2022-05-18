#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2016
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

import collections

dlg_bool_param = collections.namedtuple("dlg_bool_param", "description default_value")
dlg_int_param = collections.namedtuple("dlg_int_param", "description default_value")
dlg_float_param = collections.namedtuple("dlg_float_param", "description default_value")
dlg_string_param = collections.namedtuple(
    "dlg_string_param", "description default_value"
)
dlg_enum_param = collections.namedtuple("dlg_enum_param", "cls description default_value")
dlg_list_param = collections.namedtuple("dlg_list_param", "description default_value")
dlg_dict_param = collections.namedtuple("dlg_dict_param", "description default_value")


class dlg_batch_input(object):
    def __init__(self, mime_type, input_drop_class):
        self.input_drop_class = input_drop_class
        self.mime_type = mime_type


class dlg_batch_output(object):
    def __init__(self, mime_type, output_drop_class):
        self.output_drop_class = output_drop_class
        self.mime_type = mime_type


class dlg_streaming_input(object):
    def __init__(self, mime_type):
        self.mime_type = mime_type


class dlg_component(object):
    def __init__(
        self, name, description, batch_inputs, batch_outputs, streaming_inputs
    ):
        self.name = name
        self.description = description
        self.batch_inputs = batch_inputs
        self.batch_outputs = batch_outputs
        self.streaming_inputs = streaming_inputs
