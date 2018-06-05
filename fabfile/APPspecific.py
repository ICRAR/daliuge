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
import contextlib
import functools
import httplib
import os
import tempfile
import urllib2

from fabric.context_managers import settings, cd
from fabric.contrib.files import exists
from fabric.decorators import task, parallel
from fabric.operations import local, put
from fabric.state import env
from fabric.utils import abort

from config import APP, APP_DATAFILES, APP_INSTALL_DIR_NAME, APP_PYTHON_VERSION
from config import APP_ROOT_DIR_NAME, APP_SRC_DIR_NAME, APP_USER
from pkgmgr import install_system_packages, check_brew_port, check_brew_cellar
from system import check_dir, download, check_command, \
    create_user, get_linux_flavor, python_setup, check_python, \
    MACPORT_DIR
from utils import is_localhost, home, default_if_empty, sudo, run, success,\
    info

# Don't re-export the tasks imported from other modules, only the ones defined
# here
__all__ = [
    'start_APP_and_check_status',
    'virtualenv_setup',
    'install_user_profile',
    'copy_sources',
]


def APP_name():
    default_if_empty(env, 'APP_NAME', APP)
    return env.APP_NAME


def APP_user():
    default_if_empty(env, 'APP_USER', APP_USER)
    return env.APP_USER


def APP_install_dir():
    key = 'APP_INSTALL_DIR'
    if key not in env:
        env[key] = os.path.abspath(os.path.join(home(), APP_INSTALL_DIR_NAME))
    return env[key]


def APP_overwrite_installation():
    key = 'APP_OVERWRITE_INSTALLATION'
    return key in env


def APP_use_custom_pip_cert():
    key = 'APP_USE_CUSTOM_PIP_CERT'
    return key in env


def APP_root_dir():
    key = 'APP_ROOT_DIR'
    if key not in env:
        env[key] = os.path.abspath(os.path.join(home(), APP_ROOT_DIR_NAME))
    return env[key]


def APP_overwrite_root():
    key = 'APP_OVERWRITE_ROOT'
    return key in env


def APP_source_dir():
    key = 'APP_SRC_DIR'
    if key not in env:
        env[key] = os.path.abspath(os.path.join(home(), APP_SRC_DIR_NAME))
    return env[key]


def APP_doc_dependencies():
    key = 'APP_NO_DOC_DEPENDENCIES'
    return key in env


def has_local_git_repo():
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    return os.path.exists(os.path.join(repo_root, '.git'))


def default_APP_revision():
    if has_local_git_repo():
        return local('git rev-parse --abbrev-ref HEAD', capture=True)
    return 'local'


def APP_revision():
    default_if_empty(env, 'APP_REV', default_APP_revision)
    return env.APP_REV


def extra_python_packages():
    key = 'APP_EXTRA_PYTHON_PACKAGES'
    if key in env:
        return env.pkgs[key].split(',')
    return None


def virtualenv(command, **kwargs):
    """
    Just a helper function to execute commands in the APP virtualenv
    """
    nid = APP_install_dir()
    return run('source {0}/bin/activate && {1}'.format(nid, command), **kwargs)


def start_APP_and_check_status(tgt_cfg):
    """
    Starts the APP daemon process and checks that the server is up and running
    then it shuts down the server
    """
    # We sleep 2 here as it was found on Mac deployment to docker container
    # that the shell would exit before the APPDaemon could detach, thus
    # resulting in no startup self.
    #
    # Please replace following line with something meaningful
    # virtualenv('ngamsDaemon start -cfg {0} && sleep 2'.format(tgt_cfg))
    pass


@task
def virtualenv_setup():
    """
    Creates a new virtualenv that will hold the APP installation
    """
    APPInstallDir = APP_install_dir()
    if check_dir(APPInstallDir):
        overwrite = APP_overwrite_installation()
        if not overwrite:
            msg = ("%s exists already. Specify APP_OVERWRITE_INSTALLATION "
                   "to overwrite, or a different APP_INSTALL_DIR location")
            abort(msg % (APPInstallDir,))
        run("rm -rf %s" % (APPInstallDir,))

    # Check which python will be bound to the virtualenv
    ppath = check_python()
    if not ppath:
        ppath = python_setup(os.path.join(home(), 'python'))

    # Use our create_venv.sh script to create the virtualenv
    # It already handles the download automatically if no virtualenv command is
    # found in the system, and also allows to specify a python executable path
    with cd(APP_source_dir()):
        run("fabfile/create_venv.sh -p {0} {1}".format(ppath, APPInstallDir))

    # Download this particular certifcate; otherwise pip complains
    # in some platforms
    if APP_use_custom_pip_cert():
        if not(check_dir('~/.pip')):
            run('mkdir ~/.pip')
            with cd('~/.pip'):
                download('http://curl.haxx.se/ca/cacert.pem')
        run('echo "[global]" > ~/.pip/pip.conf; echo '
            '"cert = {0}/.pip/cacert.pem" >> ~/.pip/pip.conf;'.format(home()))

    # Update pip and install wheel; this way we can install binary wheels from
    # PyPI if available (like astropy)
    # TODO: setuptools and python-daemon are here only because
    #       python-daemon 2.1.2 is having a problem to install via setuptools
    #       but not via pip (see https://pagure.io/python-daemon/issue/2 and
    #       https://pagure.io/python-daemon/issue/3).
    #       When this problem is fixed we'll fix our dependency on python-daemo
    #       to avoid this issue entirely
    virtualenv('pip install -U pip wheel setuptools python-daemon')

    success("Virtualenv setup completed")


@task
def install_user_profile():
    """
    Put the activation of the virtualenv into the login profile of the user
    unless the APP_DONT_MODIFY_BASHPROFILE environment variable is defined

    NOTE: This will be executed for the user running APP.
    """
    if run('echo $APP_DONT_MODIFY_BASHPROFILE') or \
       'APP_NO_BASH_PROFILE' in env:
        return

    nid = APP_install_dir()
    nrd = APP_root_dir()
    with cd("~"):
        if not exists(".bash_profile_orig"):
            run('cp .bash_profile .bash_profile_orig', warn_only=True)
        else:
            run('cp .bash_profile_orig .bash_profile')

        script = ('if [ -f "{0}/bin/activate" ]'.format(nid),
                  'then',
                  '   source "{0}/bin/activate"'.format(nid),
                  'fi',
                  'export APP_PREFIX="{0}"'.format(nrd))

        run("echo '{0}' >> .bash_profile".format('\n'.join(script)))

    success("~/.bash_profile edited for automatic virtualenv sourcing")


def APP_build_cmd(no_client, develop, no_doc_dependencies):

    build_cmd = []

    build_cmd.append('cd %s ;' % APP_source_dir())
    build_cmd.append('pip install .')

    return ' '.join(build_cmd)


def build_APP():
    """
    Builds and installs APP into the target virtualenv.
    """
    with cd(APP_source_dir()):
        extra_pkgs = extra_python_packages()
        if extra_pkgs:
            virtualenv('pip install %s' % ' '.join(extra_pkgs))
        develop = False
        no_doc_dependencies = APP_doc_dependencies()
        build_cmd = APP_build_cmd(False, develop, no_doc_dependencies)
        print build_cmd
        if build_cmd != '':
            virtualenv(build_cmd)
    success("{0} built and installed".format(APP))


def prepare_APP_data_dir():
    """Creates a new APP root directory"""

    info('Preparing {0} root directory'.format(APP))
    nrd = APP_root_dir()
    # tgt_cfg = os.path.join(nrd, 'cfg', 'ngamsServer.conf')
    tgt_cfg = None
    res = run('mkdir {0}'.format(nrd))
    with cd(APP_source_dir()):
        for d in APP_DATAFILES:
            res = run('scp -r {0} {1}/.'.format(d, nrd), quiet=True)
        if res.succeeded:
            success("{0} data directory ready".format(APP))
            return tgt_cfg

    # Deal with the errors here
    error = '{0} root directory preparation under {1} failed.\n'.format(APP,
                                                                        nrd)
    if res.return_code == 2:
        error = (nrd + " already exists. Specify APP_OVERWRITE_ROOT to "
                 "overwrite, or a different APP_ROOT_DIR location")
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
    opt_file = '/etc/sysconfig/{0}'.format(APP_name())
    if get_linux_flavor() in ('Ubuntu', 'Debian'):
        opt_file = '/etc/default/{0}'.format(APP_name())

    # Script file installation
    sudo('cp {0}/fabfile/init/sysv/{1}-server /etc/init.d/'.format(nsd,
                                                                   APP_name()))
    sudo('chmod 755 /etc/init.d/{0}-server'.format(APP_name()))

    # Options file installation and edition
    sudo('cp {0}/fabfile/init/sysv/APP-server.options {1}'.format(nsd,
                                                                  opt_file))
    sudo('chmod 644 %s' % (opt_file,))

    # Enabling init file on boot
    if check_command('update-rc.d'):
        sudo('update-rc.d {0}-server defaults'.format(APP_name()))
    else:
        sudo('chkconfig --add {0}-server'.format(APP_name()))

    success("{0} init script installed".format(APP_name()))


def create_sources_tarball(tarball_filename):
    # Make sure we are git-archivin'ing from the root of the repository,
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if has_local_git_repo():
        local('cd {0}; git archive -o {1} {2}'.format(repo_root,
                                                      tarball_filename,
                                                      APP_revision()))
    else:
        local('cd {0}; tar czf {1} .'.format(repo_root, tarball_filename))


@task
def copy_sources():
    """
    Creates a copy of the APP sources in the target host.
    """

    # We still don't open the git repository to the world, so for the time
    # being we always make a tarball from our repository and copy it over
    # ssh to the remote host, where we expand it back

    nsd = APP_source_dir()

    # Because this could be happening in parallel in various machines
    # we generate a tmpfile locally, but the target file is the same
    local_file = tempfile.mktemp(".tar.gz")
    create_sources_tarball(local_file)

    # transfer the tar file if not local
    if not is_localhost():
        target_tarfile = '/tmp/{0}_tmp.tar'.format(APP_name())
        put(local_file, target_tarfile)
    else:
        target_tarfile = local_file

    # unpack the tar file into the APP_src_dir
    # (mind the "p", to preserve permissions)
    run('mkdir -p {0}'.format(nsd))
    with cd(nsd):
        run('tar xpf {0}'.format(target_tarfile))
        if not is_localhost():
            run('rm {0}'.format(target_tarfile))

    # Cleaning up now
    local('rm {0}'.format(local_file))

    success("{0} sources copied".format(APP_name()))


@parallel
def prepare_install_and_check():

    # Install system packages and create user if necessary
    nuser = APP_user()
    install_system_packages()
    create_user(nuser)
    # postfix_config()

    # Go, go, go!
    with settings(user=nuser):
        nsd, cfgfile = install_and_check()

    # Install the /etc/init.d script for automatic start
    install_sysv_init_script(nsd, nuser, cfgfile)


@parallel
def install_and_check():
    """
    Creates a virtualenv, installs APP on it,
    starts APP and checks that it is running
    """
    copy_sources()
    virtualenv_setup()
    build_APP()
    tgt_cfg = prepare_APP_data_dir()
    install_user_profile()
    start_APP_and_check_status(tgt_cfg)
    return APP_source_dir(), tgt_cfg


def upload_to(host, filename, port=7777):
    """
    Simple method to upload a file into NGAS
    """
    with contextlib.closing(httplib.HTTPConnection(host, port)) as conn:
        conn.putrequest('POST', '/QARCHIVE?filename=%s' % (
            urllib2.quote(os.path.basename(filename)),))
        conn.putheader('Content-Length', os.stat(filename).st_size)
        conn.endheaders()
        with open(filename) as f:
            for data in iter(functools.partial(f.read, 4096), ''):
                conn.send(data)
        r = conn.getresponse()
        if r.status != httplib.OK:
            raise Exception("Error while QARCHIVE-ing %s to %s:%d:\nStatus: "
                            "%d\n%s\n\n%s" % (filename, conn.host, conn.port,
                                              r.status, r.msg, r.read()))
        else:
            success("{0} successfully archived to {1}!".format(filename, host))
