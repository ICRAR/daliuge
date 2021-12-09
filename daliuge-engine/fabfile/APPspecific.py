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
Main module where application-specific tasks are carried out, like copying its
sources, installing it and making sure it works after starting it.

NOTE: This requires modifications for the specific application where this
fabfile is used. Please make sure not to use it without those modifications.
"""
import os, sys
from fabric.state import env
from fabric.colors import red
from fabric.operations import local
from fabric.decorators import task, parallel
from fabric.context_managers import settings, cd
from fabric.contrib.files import exists, sed
from fabric.utils import abort
from fabric.contrib.console import confirm

# import urllib2

# >>> All the settings below are kept in the special fabric environment
# >>> dictionary called env. Don't change the names, only adjust the
# >>> values if necessary. The most important one is env.APP_NAME, which
# is controlling most of the rest.

# The following variable will define the Application name as well as directory
# structure and a number of other application specific names.
env.APP_NAME = "DALIUGE"

# The username to use by default on remote hosts where APP is being installed
# This user might be different from the initial username used to connect to the
# remote host, in which case it will be created first
env.APP_USER = env.APP_NAME.lower()

# Name of the directory where APP sources will be expanded on the target host
# This is relative to the APP_USER home directory
env.APP_SRC_DIR_NAME = env.APP_NAME.lower() + "_src"

# Name of the directory where APP root directory will be created
# This is relative to the APP_USER home directory
env.APP_ROOT_DIR_NAME = env.APP_NAME.upper()

# Name of the directory where a virtualenv will be created to host the APP
# software installation, plus the installation of all its related software
# This is relative to the APP_USER home directory
env.APP_INSTALL_DIR_NAME = env.APP_NAME.lower() + "_rt"

# Version of Python required for the Application
# env.APP_PYTHON_VERSION = '2.7'
env.APP_PYTHON_VERSION = "3.6"

# URL to download the correct Python version
# env.APP_PYTHON_URL = 'https://www.python.org/ftp/python/2.7.14/Python-2.7.14.tgz'
env.APP_PYTHON_URL = "https://www.python.org/ftp/python/3.6.7/Python-3.6.7.tgz"

env.APP_DATAFILES = []
# >>> The following settings are only used within this APPspecific file, but may be
# >>> passed in through the fab command line as well, which will overwrite the
# >>> defaults below.

defaults = {}

# AWS specific settings
env.AWS_PROFILE = "NGAS"
env.AWS_REGION = "us-east-1"
env.AWS_AMI_NAME = "Amazon"
env.AWS_INSTANCES = 1
env.AWS_INSTANCE_TYPE = "t2.micro"
env.AWS_KEY_NAME = "icrar_{0}".format(env.APP_USER)
env.AWS_SEC_GROUP = "DALIUGE"  # Security group
env.AWS_SEC_GROUP_PORTS = [22, 80, 8000, 8001]  # ports to open
env.AWS_SUDO_USER = "ec2-user"  # required to install init scripts.

# Alpha-sorted packages per package manager
env.pkgs = {
    "YUM_PACKAGES": [
        "wget",
        "tar",
        "git",
        "gcc",
        "python36-devel",  # this is the latest available with AWS Linux
    ],
    "APT_PACKAGES": [
        "tar",
        "wget",
        "gcc",
    ],
    "SLES_PACKAGES": [
        "wget",
        "gcc",
    ],
    "BREW_PACKAGES": [
        "wget",
        "gcc",
    ],
    "PORT_PACKAGES": [
        "wget",
        "gcc",
    ],
    "APP_EXTRA_PYTHON_PACKAGES": [],
}

# Don't re-export the tasks imported from other modules, only the ones defined
# here
__all__ = ["cleanup"]

# Set the rpository root to be relative to the location of this file.
env.APP_repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# >>> The following lines need to be after the definitions above!!!
from fabfileTemplate.utils import sudo, info, success, default_if_empty, home, run
from fabfileTemplate.utils import overwrite_defaults, failure
from fabfileTemplate.system import check_command, get_linux_flavor, MACPORT_DIR
from fabfileTemplate.APPcommon import virtualenv, APP_doc_dependencies, APP_source_dir
from fabfileTemplate.APPcommon import (
    APP_root_dir,
    extra_python_packages,
    APP_user,
    build,
)
from fabfileTemplate.pkgmgr import check_brew_port, check_brew_cellar

# get the settings from the fab environment if set on command line
settings = overwrite_defaults(defaults)


def start_APP_and_check_status():
    """
    Starts the APP daemon process and checks that the server is up and running
    then it shuts down the server
    """
    virtualenv("dlg --help")
    success("dlg help is working...")


def sysinitstart_APP_and_check_status():
    """
    Starts the APP daemon process and checks that the server is up and running
    then it shuts down the server
    """
    sudo("service dlg-nm start")
    sudo("service dlg-dim start")


def APP_build_cmd():

    # The installation of the bsddb package (needed by ngamsCore) is in
    # particular difficult because it requires some flags to be passed on
    # (particularly if using MacOSX's port
    # >>>> NOTE: This function potentially needs heavy customisation <<<<<<
    build_cmd = []
    # linux_flavor = get_linux_flavor()

    build_cmd.append("cd {0} ;".format(env.APP_SRC_DIR_NAME))
    build_cmd.append("pip install .")

    return " ".join(build_cmd)


def prepare_APP_data_dir():
    """Creates a new APP root directory"""

    info("Preparing {0} root directory".format(env.APP_NAME))
    nrd = APP_root_dir()
    # tgt_cfg = os.path.join(nrd, 'cfg', 'ngamsServer.conf')
    tgt_cfg = None
    res = run("mkdir -p {0}".format(nrd))
    with cd(APP_source_dir()):
        for d in env.APP_DATAFILES:
            res = run("scp -r {0} {1}/.".format(d, nrd), quiet=True)
        if res.succeeded:
            success("{0} data directory ready".format(env.APP_NAME))
            return tgt_cfg

    # Deal with the errors here
    error = "{0} root directory preparation under {1} failed.\n".format(
        env.APP_NAME, nrd
    )
    if res.return_code == 2:
        error = (
            nrd + " already exists. Specify APP_OVERWRITE_ROOT to "
            "overwrite, or a different APP_ROOT_DIR location"
        )
    else:
        error = res
    abort(error)


def install_sysv_init_script(nsd, nuser, cfgfile):
    """
    Install the APP init script for an operational deployment.
    The init script is an old System V init system.
    In the presence of a systemd-enabled system we use the update-rc.d tool
    to enable the script as part of systemd (instead of the System V chkconfig
    tool which we use instead). The script is prepared to deal with both tools.
    """

    # Different distros place it in different directories
    # The init script is prepared for both
    opt_file = "/etc/sysconfig/dlg"
    if get_linux_flavor() in ("Ubuntu", "Debian"):
        opt_file = "/etc/default/dlg"

    # Script file installation
    sudo("cp {0}/fabfile/init/sysv/dlg-* /etc/init.d/".format(nsd))
    sudo("chmod 755 /etc/init.d/dlg-*")

    # Options file installation and edition
    sudo("cp {0}/fabfile/init/sysv/dlg.options {1}".format(nsd, opt_file))
    sudo("chmod 644 %s" % (opt_file,))

    # Enabling init file on boot
    if check_command("update-rc.d"):
        sudo("update-rc.d dlg-nm defaults")
        sudo("update-rc.d dlg-dim defaults")
    else:
        sudo("chkconfig --add dlg-nm")
        sudo("chkconfig --add dlg-dim")

    success("{0} init script installed".format(env.APP_NAME))


@task
@parallel
def cleanup():
    run("rm -rf daliuge_*")
    run("rm -rf DALIUGE")
    run("if [ -f .bash_profile.orig ]; then mv .bash_profile.orig .bash_profile; fi")


def install_docker_compose():
    pass


env.build_cmd = APP_build_cmd
env.APP_init_install_function = install_sysv_init_script
env.APP_start_check_function = start_APP_and_check_status
env.sysinitAPP_start_check_function = sysinitstart_APP_and_check_status
env.prepare_APP_data_dir = prepare_APP_data_dir
env.APP_extra_sudo_function = install_docker_compose
