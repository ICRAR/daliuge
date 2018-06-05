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
Module containing system-level utility methods and fabric tasks
"""
import os
import urlparse

from fabric.colors import blue, green
from fabric.context_managers import cd, settings
from fabric.decorators import task
from fabric.operations import prompt
from fabric.state import env
from fabric.utils import puts, abort
import pkg_resources

from utils import run, sudo, get_public_key
from config import APP_PYTHON_VERSION

# List of supported OSes
SUPPORTED_OS_LINUX = [
    'Amazon Linux',
    'Amazon',
    'CentOS',
    'Ubuntu',
    'Debian',
    'Suse',
    'SUSE',
    'openSUSE',
    'SLES-SP2',
    'SLES-SP3'
]

SUPPORTED_OS_MAC = [
    'Darwin',
]

SUPPORTED_OS = []
SUPPORTED_OS += SUPPORTED_OS_LINUX
SUPPORTED_OS += SUPPORTED_OS_MAC

# The directory under which MaxOSX's 'port' installs stuff
MACPORT_DIR = '/opt/local'

@task
def check_command(command, *args, **kwargs):
    """
    Check existence of command remotely
    """
    res = run('if command -v {0} &> /dev/null ;then command -v {0};else echo ;fi'.format(command), *args, **kwargs)
    return res

@task
def check_dir(directory):
    """
    Check existence of remote directory
    """
    res = run("""if [ -d {0} ]; then echo 1; else echo ; fi""".format(directory))
    return res

@task
def check_path(path):
    """
    Check existence of remote path
    """
    res = run('if [ -e {0} ]; then echo 1; else echo 0; fi'.format(path))
    return res

@task
def check_user(user):
    """
    Task checking existence of user
    """
    res = run('if id -u "{0}" >/dev/null 2>&1; then echo 1; else echo 0; fi;'.format(user))
    if res == '0':
        puts('User {0} does not exist'.format(user))
        return False
    else:
        return True

@task
def check_sudo():
    '''
    Checks if the sudo command is present.
    Installing it via the package manager of choice is too late already,
    because those (e.g., yum, apt-get, etc.) are invoked via sudo already.
    '''
    if not check_command('sudo', quiet=True):
        abort('sudo is not installed in the target system')

def get_linux_flavor():
    """
    Obtain and set the env variable linux_flavor
    """

    # Already ran through this method
    if env.has_key('linux_flavor'):
        return env.linux_flavor

    linux_flavor = None
    # Try lsb_release
    if check_command('lsb_release'):
        distributionId = run('lsb_release -i')
        if distributionId and distributionId.find(':') != -1:
            linux_flavor = distributionId.split(':')[1].strip()

    # Try python
    if not linux_flavor and check_command('python'):
        lf = run("python -c 'import platform; print platform.linux_distribution()[0]'")
        if lf:
            linux_flavor = lf.split()[0]

    # Try /etc/issue
    if not linux_flavor and check_path('/etc/issue') == '1':
        re = run('cat /etc/issue')
        issue = re.split()
        if issue:
            if issue[0] == 'CentOS' or issue[0] == 'Ubuntu' \
               or issue[0] == 'Debian':
                linux_flavor = issue[0]
            elif issue[0] == 'Amazon':
                linux_flavor = ' '.join(issue[:2])
            elif issue[2] in ('SUSE', 'openSUSE'):
                linux_flavor = issue[2]

    # Try uname -s
    if not linux_flavor:
        linux_flavor = run('uname -s')

    # Sanitize
    if linux_flavor and type(linux_flavor) == type([]):
        linux_flavor = linux_flavor[0]

    # Final check
    if not linux_flavor or linux_flavor not in SUPPORTED_OS:
        puts('>>>>>>>>>>')
        puts('Target machine is running an unsupported or unkown Linux flavor: {0}.'.format(linux_flavor))
        puts('If you know better, please enter it below.')
        puts('Must be one of:')
        puts(' '.join(SUPPORTED_OS))
        linux_flavor = prompt('LINUX flavor: ')

    puts(blue("Remote machine running %s" % linux_flavor))
    env.linux_flavor = linux_flavor
    return linux_flavor

@task
def assign_ddns():
    """
    Installs the noip ddns client to the specified host.
    After the installation the configuration step is executed and that
    requires some manual input. Then the noip2 client is started in background.
    """
    with cd('/usr/local/src'):
        download('http://www.no-ip.com/client/linux/noip-duc-linux.tar.gz',
                 root=True)
        sudo('tar xf noip-duc-linux.tar.gz')
        sudo('cd noip-2.1.9-1')
        sudo('make install')
        sudo('noip2 -C')
    # TODO: put correct startup script in repo and install it
    # sudo('cp redhat.noip.sh /etc/init.d/noip')
    # sudo('chmod a+x /etc/init.d/noip')
    # sudo('chkconfig noip on')
    # sudo('service noip start')
    puts(green("\n***** Dynamic IP address assigned ******\n"))


@task
def postfix_config():
    """
    Setup a valid Postfix configuration to be used by APP.
    """
    if 'gmail_account' not in env:
        prompt('GMail Account:', 'gmail_account')
    if 'gmail_password' not in env:
        prompt('GMail Password:', 'gmail_password')

    # Make sure postfix is there and not sendmail
    sudo('service sendmail stop')
    sudo('service postfix stop')
    sudo('chkconfig sendmail off')
    sudo('chkconfig sendmail --del')
    sudo('chkconfig postfix --add')
    sudo('chkconfig postfix on')

    # Set up the main configuration file and the password file
    puts(pkg_resources.resource_filename(__name__, 'main.cf'), '/etc/postfix/main.cf')  # @UndefinedVariable
    sudo('echo "[smtp.gmail.com]:587 {0}@gmail.com:{1}" > /etc/postfix/sasl_passwd'.format(env.gmail_account, env.gmail_password))
    sudo('chmod 400 /etc/postfix/sasl_passwd')
    sudo('postmap /etc/postfix/sasl_passwd')

    # Start it
    sudo('service postfix start')

def download(url, target=None, root=False):
    if target is None:
        parts = urlparse.urlparse(url)
        target = parts.path.split('/')[-1]
    if check_command('wget'):
        cmd = 'wget --no-check-certificate -q -O {0} {1}'.format(target, url)
    elif check_command('curl'):
        cmd = 'curl -o {0} {1}'.format(target, url)
    else:
        raise Exception("Neither wget nor curl are installed")
    if root:
        sudo(cmd)
    else:
        run(cmd)
    return target


@task
def check_python():
    """
    Check for the existence of correct version of python
    """
    return check_command('python{0}'.format(APP_PYTHON_VERSION))

@task
def python_setup(ppath):
    """
    Ensure that there is the right version of python available
    If not install it from scratch in user directory.
    """
    with cd('/tmp'):
        download(APP_PYTHON_URL)
        base = os.path.basename(APP_PYTHON_URL)
        pdir = os.path.splitext(base)[0]
        run('tar -xzf {0}'.format(base))
    with cd('/tmp/{0}'.format(pdir)):
        puts('Python BUILD log-file can be found in: /tmp/py_install.log')
        puts(green('Configuring Python.....'))
        run('./configure --prefix {0} > /tmp/py_install.log 2>&1;'.format(ppath))
        puts(green('Building Python.....'))
        run('make >> /tmp/py_install.log 2>&1;')
        puts(green('Installing Python.....'))
        run('make install >> /tmp/py_install.log 2>&1')
        ppath = '{0}/bin/python{1}'.format(ppath,APP_PYTHON_VERSION)
    return ppath

def get_fab_public_key():
    """
    Returns the public key material for the private key currently being used
    by fab (if any)
    """

    # If the user specified a private key via "fab -i" we use that one;
    # otherwise we use the default RSA key.
    # env.key_filename can be a list, so make sure we handle it correctly
    if 'key_filename' in env and env.key_filename:
        k_fname = env.key_filename
        if not isinstance(k_fname, list):
            fnames = [k_fname]
        else:
            fnames = k_fname
    else:
        fnames = [os.path.expanduser("~/.ssh/id_rsa")]

    for key_filename in fnames:
        if key_filename is not None and os.path.exists(key_filename):
            puts("Obtaining public key for private file %s" % (key_filename,))
            pub_key = get_public_key(key_filename)
            if pub_key:
                puts("Public key obtained")
                return pub_key

@task
def create_user(user):
    """
    Creates a user in the system.
    """

    # TODO: Check if the user exists
    #       Also, these commands are linux-specific,
    #       there are others that work on MacOS
    group = user.lower()
    sudo('groupadd ' + group, warn_only=True)
    sudo('useradd -g {0} -m -s /bin/bash {1}'.format(group, user), warn_only=True)
    sudo('mkdir /home/{0}/.ssh'.format(user), warn_only=True)
    sudo('chmod 700 /home/{0}/.ssh'.format(user))
    sudo('chown -R {0}:{1} /home/{0}/.ssh'.format(user, group))

    # if docker is installed add the APP user to the group.
    sudo('rpm -q docker >/dev/null 2>&1; if [ $? -eq 0 ]; then usermod -aG docker {0}; fi'.
         format(user))

    # Copy the public key of our SSH key if we're using one
    public_key = get_fab_public_key()
    if public_key:
        sudo("echo '{0}' >> /home/{1}/.ssh/authorized_keys".format(public_key, user))
        sudo('chmod 600 /home/{0}/.ssh/authorized_keys'.format(user))
        sudo('chown {0}:{1} /home/{0}/.ssh/authorized_keys'.format(user, group))

    # openSUSE creates a suboptimal ~/.profile because it shows an error message
    # if /etc/profile doesn't exist (which is the case on openSUES dockers),
    # so we comment out that particular line
    if get_linux_flavor() == 'openSUSE':
        with settings(user=user):
            run('''sed -i 's/^test -z "$PROFILEREAD".*/#\\0/' ~/.profile ''')
