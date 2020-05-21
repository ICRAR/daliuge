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

import os
import uuid

from ..drop import ContainerDROP
from ..drop import FileDROP
from dlg.meta import dlg_string_param, dlg_list_param, dlg_component, \
    dlg_batch_input, dlg_batch_output, dlg_streaming_input


class FileImportApp(ContainerDROP):
    """
    Recursively scans a directory (dirname) and checks for files with
    a particular extension (ext). If a match is made then a FileDROP
    is created which contains the path to the file. The FileDROP is then added
    to the FileImportApp (ContainerDROP)
    """
    compontent_meta = dlg_component('FileImportApp', 'Recursively scans a directory (dirname) and checks for files with '
                                    'a particular extension (ext). If a match is made then a FileDROP '
                                    'is created which contains the path to the file. The FileDROP is then added '
                                    'to the FileImportApp (ContainerDROP)',
                                    [dlg_batch_input('binary/*', [])],
                                    [dlg_batch_output('binary/*', [])],
                                    [dlg_streaming_input('binary/*')])

    dirname = dlg_string_param('dirname', None)
    ext = dlg_list_param('ext', [])

    def initialize(self, **kwargs):
        super(ContainerDROP, self).initialize(**kwargs)
        self._children = []

        if not self.dirname:
            raise Exception('dirname not defined')
        if not os.path.isdir(self.dirname):
            raise Exception('%s is not a directory' % (self.dirname))

        if not self.ext:
            raise Exception('ext not defined')
        self._ext = [x.lower() for x in self.ext]
        self._scan_and_import_files()

    def _scan_and_import_files(self):
        for root, dirs, files in os.walk(self.dirname):
            for f in files:
                _, ext = os.path.splitext(f)
                if ext.lower() in self._ext:
                    path = '{0}/{1}'.format(root, f)
                    fd = FileDROP(str(uuid.uuid1()),
                                  str(uuid.uuid1()),
                                  filepath = path,
                                  check_filepath_exists = True)
                    self._children.append(fd)
                    fd._parent = self
