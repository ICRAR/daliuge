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
Module containing methods and fabric tasks that manage system packages
"""

from fabric.colors import red
from fabric.context_managers import settings, hide
from fabric.decorators import task
from fabric.state import env
from fabric.utils import puts, abort

from system import check_command, get_linux_flavor
from utils import sudo, run

# Don't re-export the tasks imported from other modules, only ours
__all__ = ['install_homebrew', 'install_system_packages', 'system_check',
           'list_packages']


def extra_packages():
    key = 'APP_EXTRA_PACKAGES'
    if key in env:
        return env[key].split(',')
    return []


def install_yum(packages):
    """
    Install packages using YUM
    """
    errmsg = sudo('yum --assumeyes --quiet install {0}'.
                  format(' '.join(packages + extra_packages())),
                  combine_stderr=True, warn_only=True)
    processCentOSErrMsg(errmsg)


def processCentOSErrMsg(errmsg):
    if (errmsg is None or len(errmsg) == 0):
        return
    if (errmsg == 'Error: Nothing to do'):
        return
    firstKey = errmsg.split()[0]
    if (firstKey == 'Error:'):
        abort(errmsg)


def install_zypper(packages):
    """
    Install packages using zypper (SLES)
    """
    sudo('zypper --non-interactive install {0}'.
         format(' '.join(packages + extra_packages())),
         combine_stderr=True, warn_only=True)


def install_apt(packages):
    """
    Install packages using APT
    """
    # We need to iterate over each one because if at least one of them
    # is actually not a package (misspelled, doesn't exist anymore, debian-
    # or ubuntu-specific, etc) the whole install process would fail
    # On the other hand there appears to be no flag to ignore these errors
    # on apt-get (tested on Ubuntu 12.04)
    for pkg in packages + extra_packages():
        sudo('apt-get -qq -y install {0}'.format(pkg))


def install_brew(package):
    """
    Install a package using homebrew (Mac OSX)
    """
    with settings(warn_only=True):
        run('export HOMEBREW_NO_EMOJI=1; brew install {0} | grep -v "\%"'.
            format(package))


def install_port(package):
    """
    Install a package using macports (Mac OSX)
    """
    with settings(warn_only=True):
        run('sudo port install {0}'.format(package))


@task
def install_homebrew():
    """
    Task to install homebrew on Mac OSX.

    NOTE: This should not be done if macports is installed already.
    """
    lf = get_linux_flavor()
    if lf != 'Darwin':
        puts(red("Potentially this is not a Mac OSX installation: {0}".
                 format(lf)))
        raise(ValueError)
    if check_command('port'):
        puts(red('MacPorts is installed and it is not recommended to mix it with homebrew!!'))
        puts(red('Bailing out!'))
        raise(ValueError)
        return
    if not check_command('brew'):
        run('ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"')
    else:
        puts(red('Homebrew is installed already! New installation not required.'))


def check_yum(package):
    """
    Check whether package is installed or not

    NOTE: requires sudo access to machine
    """
    with hide('stdout', 'running', 'stderr'):
        res = sudo('yum --assumeyes --quiet list installed {0}'.
                   format(package), combine_stderr=True, warn_only=True)
    # print res
    if res.find(package) > 0:
        print "Installed package {0}".format(package)
        return True
    else:
        print "NOT installed package {0}".format(package)
        return False


def check_apt(package):
    """
    Check whether package is installed using APT

    NOTE: This requires sudo access
    """
    # TODO
    with hide('stdout', 'running'):
        res = sudo('dpkg -L | grep {0}'.format(package))
    if res.find(package) > -1:
        print "Installed package {0}".format(package)
        return True
    else:
        print "NOT installed package {0}".format(package)
        return False


def check_brew_port():
    """
    Check for existence of homebrew or macports

    RETRUNS: string containing the installed package manager or None
    """
    if check_command('brew'):
        return 'brew'
    elif check_command('port'):
        return 'port'
    else:
        return None


def check_brew_cellar():
    """
    Find the brewing cellar (Mac OSX)
    """
    with hide('output'):

        # This yields something like "Homebrew 1.4.2-14-g3e99504"
        # but might be different in earlier versions (or newer)
        # so we do what we can to clean it up
        version = run('brew --version')
        if 'Homebrew ' in version:
            version = version.split()[1].strip()
        if '-' in version:
            version = version.split('-')[0].strip()

        version = tuple(map(int, version.split('.')))

        # I'm not sure exactly when --cellar was introduced, but it was already
        # there in 0.9.9. On the other hand HOMEBREW_CELLAR was also still
        # outputted via brew config in 0.9.9, so I think it's safe to make the
        # cut at 1.0.
        if version >= (1, 0):
            cellar = run('brew --cellar')
        else:
            cellar = run(
                'brew config | grep HOMEBREW_CELLAR').split(":")[1].strip()

    return cellar


@task
def install_system_packages():
    """
    Perform the installation of system-level packages needed by APP to work.
    """

    # Install required packages
    linux_flavor = get_linux_flavor()

    if (linux_flavor in ['CentOS', 'Amazon Linux']):
        # Update the machine completely
        errmsg = sudo('yum --assumeyes --quiet clean all', combine_stderr=True,
                      warn_only=True)
        errmsg = sudo('yum --assumeyes --quiet update', combine_stderr=True,
                      warn_only=True)
        processCentOSErrMsg(errmsg)
        install_yum(env.pkgs['YUM_PACKAGES'])
        if linux_flavor == 'CentOS':
            sudo('/etc/init.d/iptables stop')  # CentOS firewall blocks port!
    elif (linux_flavor in ['Ubuntu', 'Debian']):
        errmsg = sudo('apt-get -qq -y update', combine_stderr=True,
                      warn_only=True)
        install_apt(env.pkgs['APT_PACKAGES'])
    elif linux_flavor in ['SUSE', 'SLES-SP2', 'SLES-SP3', 'SLES', 'openSUSE']:
        errmsg = sudo('zypper -n -q patch', combine_stderr=True,
                      warn_only=True)
        install_zypper(env.pkgs['SLES_PACKAGES'])
    elif linux_flavor == 'Darwin':
        pkg_mgr = check_brew_port()
        if pkg_mgr is None:
            install_homebrew()
            pkg_mgr = 'brew'
        if pkg_mgr == 'brew':
            for package in env.pkgs['BREW_PACKAGES']:
                install_brew(package)
        elif pkg_mgr == 'port':
            for package in env.pkgs['PORT_PACKAGES']:
                install_port(package)
    else:
        abort("Unsupported linux flavor detected: {0}".format(linux_flavor))


@task
def system_check():
    """
    Check for existence of system level packages

    NOTE: This requires sudo access on the machine(s)
    """
    linux_flavor = get_linux_flavor()
    ok = True
    if (linux_flavor in ['CentOS', 'Amazon Linux']):
        ok = all([check_yum(p) for p in YUM_PACKAGES])
    elif (linux_flavor == 'Ubuntu'):
        ok = all([check_apt(p) for p in APT_PACKAGES])
    else:
        abort("Unknown linux flavor detected: {0}".format(linux_flavor))

    if ok:
        puts("All required packages are installed.")
    else:
        puts("At least one package is missing!")


@task
def list_packages():
    import pprint
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(env.pkgs)
