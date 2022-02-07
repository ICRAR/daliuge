#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2014
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
Script to generate passwd and group files for docker containers to mount.
This will make sure that the apps in the containers are running as the 
current user and thus the generated files have the correct owner.

Inspired by Stimela code
"""
import pwd, grp, os

workdir = f"{os.environ['DLG_ROOT']}/workspace/settings"
try:
    os.mkdir(workdir)
except FileExistsError:
    pass
except:
    raise
template_dir = os.path.join(os.path.dirname(__file__), ".")
# get current user info
pw = pwd.getpwuid(os.getuid())
gr = grp.getgrgid(pw.pw_gid)
with open(os.path.join(workdir, "passwd"), "wt") as file:
    file.write(open(os.path.join(template_dir, "passwd.template"), "rt").read())
    file.write(f"{pw.pw_name}:x:{pw.pw_uid}:{pw.pw_gid}:{pw.pw_gecos}:/:/bin/bash")
with open(os.path.join(workdir, "group"), "wt") as file:
    file.write(open(os.path.join(template_dir, "group.template"), "rt").read())
    file.write(f"{gr.gr_name}:x:{gr.gr_gid}:")

