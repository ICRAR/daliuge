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
"""
This package contains the modules implementing the core functionality of
the system.
"""


# It is of utmost importance that this is the *first* thing we do any time
# we load daliuge up. Most modules in daliuge that have a logger create it at
# import time, so we need to perform this setup before doing any other imports


from dlg.runtime.dlg_logging import setup_logger_class
setup_logger_class()
del setup_logger_class

from dlg.common.version import git_version as __git_version__
from dlg.common.version import version as __version__
from ..apps import get_include_dir
from ..dask_emulation import delayed
