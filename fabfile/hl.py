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
Module with a few high-level fabric tasks users are likely to use
"""

import os

from fabric.context_managers import settings
from fabric.decorators import task, parallel
from fabric.operations import local
from fabric.state import env
from fabric.tasks import execute

from aws import create_aws_instances
from dockerContainer import setup_container, create_final_image
from utils import repo_root, check_ssh, append_desc
from system import check_sudo

from APPspecific import install_and_check, prepare_install_and_check
from APPspecific import create_sources_tarball, upload_to, APP_revision


# Don't re-export the tasks imported from other modules, only ours
__all__ = ['user_deploy', 'operations_deploy', 'aws_deploy', 'docker_image',
           'prepare_release', 'upload_release']


@task
@parallel
@append_desc
def user_deploy():
    """Compiles and installs APP in a user-owned directory."""
    check_ssh()
    install_and_check()


@task
@parallel
@append_desc
def operations_deploy():
    """Performs a system-level setup on a host and installs APP on it"""
    check_ssh()
    check_sudo()
    prepare_install_and_check()


@task
@append_desc
def aws_deploy():
    """Deploy APP on fresh AWS EC2 instances."""
    # This task doesn't have @parallel because its initial work
    # (actually *creating* the target host(s)) is serial.
    # After that it modifies the env.hosts to point to the target hosts
    # and then calls execute(prepare_install_and_check) which will be parallel
    create_aws_instances()
    execute(prepare_install_and_check)


@task
@append_desc
def docker_image():
    """ Create a Docker image with an APP installation."""

    # Create the target container holding onto the container info
    # This container will be running an SSH server, and we will be able
    # to connect to its root user with our SSH key
    dockerState = setup_container()

    # Now install into the docker container.
    # The stage above sets the container's IP into env.host, so the rest of the
    # commands are effectively executed on the container
    # We disable the known hosts check since docker containers created at
    # different times might end up having the same IP assigned to them, and the
    # ssh known hosts check will fail
    # Finally, we also hardcode that the doc dependencies will *not* be
    # installed into the docker container, so we generate a thiner image
    with settings(disable_known_hosts=True, APP_NO_DOC_DEPENDENCIES=True):
        execute(prepare_install_and_check)
        create_final_image(dockerState)


@task
def prepare_release():
    """
    Prepares an APP release (deploys APP into AWS serving its own source/doc)
    """

    # Create the AWS instance
    aws_deploy()
    upload_release()


@task
def upload_release():
    """
    Uploads sources and documentation to AWS instance.
    """
    # Create and upload the sources
    sources = "APP_src-{0}.tar.gz".format(APP_revision())
    if os.path.exists(sources):
        os.unlink(sources)
    create_sources_tarball(sources)
    try:
        upload_to(env.hosts[0], sources)
    finally:
        os.unlink(sources)

    # Generate a PDF documentation and upload it too
    local("make -C %s/doc latexpdf" % (repo_root()))
    upload_to(env.hosts[0], '%s/doc/_build/latex/APP.pdf' % (repo_root()))
