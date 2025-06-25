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


import re
import subprocess


def check_k8s_env():
    """
    Makes sure kubectl can be called and is accessible.
    """
    try:
        output = subprocess.run(
            ["kubectl version"], capture_output=True, shell=True, timeout=2, check=False
        ).stdout
        output = output.decode(encoding="utf-8").replace("\n", "")
        pattern = re.compile(r"^Client Version:.*Server Version:.*")
        return re.match(pattern, output)
    except subprocess.SubprocessError:
        return False
