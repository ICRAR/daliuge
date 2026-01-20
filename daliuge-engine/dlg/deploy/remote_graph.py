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

import base64
import json
import requests
import sys

import logging
logger = logging.getLogger(f"dlg.{__name__}")

def gitlab_request(user, repo, branch, path):
    """
    Constructs a 'get' request using the GitLab API.

    See: https://docs.gitlab.com/api/repository_files/#get-file-from-repository

    GitLab requires URL-encoded paths, so we replace them with the %2F string for the
    user and path variables.

    :param user: The user or organisation (e.g. ICRAR/ or ska-telescope/sdp).
    :param repo: The name of the repository the file is in
    :param branch: The branch we want to get from
    :param path: The path to the file relative to the base directory of the repository
    :return:
    """
    url_enabled_user = user.replace("/", "%2F")
    url_enabled_path = path.replace("/", "%2F")
    r = requests.get(f"https://gitlab.com/api/v4/projects/{url_enabled_user}%2F"
                     f"{repo}/repository/files/{url_enabled_path}/raw?ref"
                     f"={branch}", timeout=15)

    return json.loads(r.content)

def github_request(user, repo, branch, path):
    """
    Constructs a 'get' request using the GitHub api

    See: https://docs.github.com/en/rest/repos/contents

    The difference here is the response content is itself a JSON dictionary, with the
    'content' element base64 encoded. We have to decode this first and then return it.

    :param user: The user or organisation (e.g. ICRAR/ or ska-telescope/sdp).
    :param repo: The name of the repository the file is in
    :param branch: The branch we want to get from
    :param path: The path to the file relative to the base directory of the repository
    :return:
    """

    def report_and_exit(request, content):
        logger.error("Things went wrong with request: %s !", {request})
        logger.error("Received content: %s", content)
        sys.exit(1)

    request_url = f"https://api.github.com/repos/{user}/{repo}/contents/{path}?ref={branch}"

    r = requests.get(request_url, timeout=15)
    content = json.loads(r.content)
    try:
        decoded_content = base64.b64decode(content['content']).decode()
        if r.status_code != 200:
            report_and_exit(request_url, decoded_content)
    except KeyError:
        report_and_exit(request_url, content)

    return json.loads(decoded_content)
