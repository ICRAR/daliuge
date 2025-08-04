#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2025
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
Utility functions to enable submitting EAGLE graphs that are hosted on a Git repository. 
"""

import os
import sys

import requests


# GitLab API:  curl "https://gitlab.com/api/v4/projects/ska-telescope%2Fsdp%2Fska-sdp-resource-model/repository/files/scripts%2Fexperiment.slurm/raw?ref=main"

# GitHub API: curl -sL https://raw.githubusercontent.com/slimtoolkit/slim/master/scripts/install-slim.sh

# https://github.com/ICRAR/EAGLE-graph-repo/blob/master/examples/HelloWorld-Universe.graph

def gitlab_request(user, repo, branch, path):
    url_enabled_user = user.replace("/", "%2F")
    url_enabled_path = path.replace("/", "%2F")
    r = requests.get(f"https://gitlab.com/api/v4/projects/{url_enabled_user}%2F"
                     f"{repo}/repository/files/{url_enabled_path}/raw?ref"
                     f"={branch}")
    return r.content

def github_request(user, repo, branch, path):
    r = requests.get(f"https://raw.githubusercontent.com/{user}/{repo}/{branch}/{path}")
    if r.status_code != 200:
        print("Things went wrong!!")
        sys.exit(1)
    return r.content
