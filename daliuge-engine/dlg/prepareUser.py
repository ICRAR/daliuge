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
Replicated from utils.py in order to be able to call it stand-alone from
run_engine.sh. This function fills template user and group files, which will
then be used inside the docker containers to ensure that the user running the
app inside the container is the same as outside.
"""

import os
import pwd
import grp
import platform


def prepareUser(DLG_ROOT="."):
    workdir = f"{DLG_ROOT}/workspace/settings"
    try:
        os.makedirs(workdir, exist_ok=True)
    except OSError as e:
        raise e
    template_dir = os.path.dirname(__file__)
    # get current user info
    pw = pwd.getpwuid(os.getuid())
    gr = grp.getgrgid(pw.pw_gid)
    if platform.system() == "Darwin":
        grpnam = "staff"
    else:
        grpnam = "docker"
    dgr = grp.getgrnam(grpnam)
    with open(os.path.join(workdir, "passwd"), "wt") as file:
        file.write(open(os.path.join(template_dir, "passwd.template"), "rt").read())
        file.write(
            f"{pw.pw_name}:x:{pw.pw_uid}:{pw.pw_gid}:{pw.pw_gecos}:{DLG_ROOT}:/bin/bash\n"
        )
    with open(os.path.join(workdir, "group"), "wt") as file:
        file.write(open(os.path.join(template_dir, "group.template"), "rt").read())
        file.write(f"{gr.gr_name}:x:{gr.gr_gid}:\n")
        file.write(f"docker:x:{dgr.gr_gid}:{pw.pw_name}\n")
        file.write(f"sudo:x:27:{gr.gr_name}\n")

    return dgr.gr_gid
