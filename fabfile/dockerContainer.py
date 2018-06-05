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
Module containing Docker related methods and tasks
"""

import collections
import io
import os
import tarfile
import time

from fabric.colors import blue
from fabric.context_managers import settings
from fabric.state import env
from fabric.tasks import execute
from fabric.utils import puts

from APPspecific import APP_root_dir, APP_user, APP_source_dir
from system import get_fab_public_key
from utils import check_ssh, generate_key_pair, run, success, failure,\
    default_if_empty, info


# Don't re-export the tasks imported from other modules
__all__ = []


DockerContainerState = collections.namedtuple('DockerContainerState', 'client container')

def docker_keep_APP_root():
    key = 'DOCKER_KEEP_APP_ROOT'
    return key in env

def docker_keep_APP_src():
    key = 'DOCKER_KEEP_APP_SRC'
    return key in env

def docker_image_repository():
    default_if_empty(env, 'DOCKER_IMAGE_REPOSITORY', 'icrar/APP')
    return env.DOCKER_IMAGE_REPOSITORY

def add_public_ssh_key(cont):

    # Generate a private/public key pair if there's not one already in use
    public_key = get_fab_public_key()
    if not public_key:
        private, public_key = generate_key_pair()
        env.key = private

    #write password to file
    tar_data = io.BytesIO()
    tarinfo = tarfile.TarInfo(name='.ssh/authorized_keys')
    tarinfo.size = len(public_key)
    tarinfo.mtime = time.time()
    with tarfile.TarFile(fileobj=tar_data, mode='w') as tar:
        tar.addfile(tarinfo, io.BytesIO(public_key))

    tar_data.seek(0)
    cont.put_archive(path='/root/', data=tar_data)


def setup_container():
    """Create and prepare a docker container and let Fabric point at it"""

    from docker.client import DockerClient

    image = 'centos:centos7'
    container_name = 'APP_installation_target'
    cli = DockerClient.from_env(version='auto', timeout=10)

    # Create and start a container using the newly created stage1 image
    cont = cli.containers.run(image=image, name=container_name, remove=False, detach=True, tty=True)
    success("Created container %s from %s" % (container_name, image))

    # Find out container IP, prepare container for APP installation
    try:
        host_ip = cli.api.inspect_container(cont.id)['NetworkSettings']['IPAddress']

        info("Updating and installing OpenSSH server in container")
        cont.exec_run('yum -y update')
        cont.exec_run('yum -y install openssh-server sudo')
        cont.exec_run('yum clean all')

        info('Configuring OpenSSH to allow connections to container')
        add_public_ssh_key(cont)
        cont.exec_run('sed -i "s/#PermitRootLogin yes/PermitRootLogin yes/" /etc/ssh/sshd_config')
        cont.exec_run('sed -i "s/#UseDNS yes/UseDNS no/" /etc/ssh/sshd_config')
        cont.exec_run('ssh-keygen -A')
        cont.exec_run('chown root.root /root/.ssh/authorized_keys')
        cont.exec_run('chmod 600 /root/.ssh/authorized_keys')
        cont.exec_run('chmod 700 /root/.ssh')

        info('Starting OpenSSH deamon in container')
        cont.exec_run('/usr/sbin/sshd -D', detach=True)
    except:
        failure("Error while preparing container for APP installation, cleaning up...")
        cont.stop()
        cont.remove()
        raise

    # From now on we connect to root@host_ip using our SSH key
    env.hosts = host_ip
    env.user = 'root'
    if 'key_filename' not in env and 'key' not in env:
        env.key_filename = os.path.expanduser("~/.ssh/id_rsa")

    # Make sure we can connect via SSH to the newly started container
    # We disable the known hosts check since docker containers created at
    # different times might end up having the same IP assigned to them, and the
    # ssh known hosts check will fail
    with settings(disable_known_hosts=True):
        execute(check_ssh)

    success('Container successfully setup! APP installation will start now')
    return DockerContainerState(cli, cont)

def cleanup_container():

    # Clean downloaded packages, remove unnecessary packages
    #
    # This is obviously a CentOS 7 hardcoded list, but we already hardcode
    # CentOS 7 as the FROM image in our build file so we are basically building
    # up on that assumption. Generalising all this logic would require quite
    # some effort. but since it is not necessarily something we need or want, it
    # is kind of ok to live with this sin.
    for pkg in ('autoconf', 'bzip2-devel', 'cpp',
                'groff-base', 'krb5-devel', 'less', 'libcom_err-devel', 'libgnome-keyring', 'libedit', 'libgomp', 'libkadm5', 'libselinux-devel', 'm4', 'mpfr', 'pcre-devel', 'rsync', 'libverto-devel', 'libmpc',
                'gcc', 'gdbm-devel', 'git',
                'glibc-devel', 'glibc-headers', 'kernel-headers', 'libdb-devel',
                'make', 'openssl-devel', 'patch', 'perl', 'postgresql',
                'postgresql-libs', 'python-devel', 'readline-devel', 'sqlite-devel',
                'sudo', 'wget', 'zlib-devel'):
        run('yum --assumeyes --quiet remove %s' % (pkg,), warn_only=True)
    run('yum clean all')

    # Remove user directories that are not needed anymore
    with settings(user=APP_user()):

        # By default we do not ship the image with a working APP directory
        to_remove = ['~/.cache']
        if not docker_keep_APP_src():
            to_remove.append(APP_source_dir())
        if not docker_keep_APP_root():
            to_remove.append(APP_root_dir())

        for d in to_remove:
            run ('rm -rf %s' % d,)

def create_final_image(state):
    """Create docker image from container"""

    puts(blue("Building image"))

    # First need to cleanup container before we stop and commit it.
    # We execute most of the commands via ssh, until we actually remove ssh
    # itself and forcefully remove unnecessary system-level folders
    execute(cleanup_container)
    cont = state.container
    cont.exec_run('yum --assume-yes remove fipscheck fipscheck-lib openssh-server')
    cont.exec_run('rm -rf /var/log')
    cont.exec_run('rm -rf /var/lib/yum')

    conf = {'Cmd': ["/usr/bin/su", "-", "APP", "-c", "/home/APP/APP_rt/bin/ngamsServer -cfg /home/APP/APP/cfg/ngamsServer.conf -autoOnline -force -v 4"]}
    image_repo = docker_image_repository()

    try:
        cont.stop()
        cont.commit(repository=image_repo, tag='latest', conf=conf)
        success("Created Docker image %s:latest" % (image_repo,))
    except Exception as e:
        failure("Failed to build final image: %s" % (str(e)))
        raise
    finally:
        # Cleanup the docker environment from all our temporary stuff
        cont.remove()
