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
"""
Fabric scripts for NGAS deployment and related activities.

For a detailed description of a task run "fab -d <task>"

End users will likely use the hl.operations_deploy or hl.user_deploy tasks,
Other tasks, including lower-level tasks, can also be invoked.
"""
from . import APPspecific
from fabfileTemplate import APPcommon
from fabfileTemplate import aws
from fabfileTemplate import hl
from fabfileTemplate import pkgmgr
from fabfileTemplate import system
from fabfileTemplate import utils

