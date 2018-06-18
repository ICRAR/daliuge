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
Various utilities used throughout the rest of the modules
"""

import math
import os
import socket
import time
import types
import urllib2

from fabric.colors import green, red, yellow, blue
from fabric.context_managers import settings, hide
from fabric.decorators import task, parallel
from fabric.exceptions import NetworkError
from fabric.operations import run as frun, sudo as fsudo
from fabric.state import env
from fabric.utils import puts, abort


def to_boolean(choice, default=False):
    """Convert the yes/no to true/false

    :param choice: the text string input
    :type choice: string
    """
    if type(choice) == types.BooleanType:
        return choice
    valid = {"True":  True,  "true": True, "yes": True, "ye": True, "y": True,
             "False": False, "false": False,  "no": False, "n": False}
    choice_lower = choice.lower()
    if choice_lower in valid:
        return valid[choice_lower]
    return default


def default_if_empty(env, key, default):
    if key not in env or not env[key]:
        if hasattr(default, '__call__'):
            env[key] = default()
        else:
            env[key] = default


@task
def whatsmyip():
    """
    Returns the external IP address of the host running fab.

    NOTE: This is only used for EC2 setups, thus it is assumed
    that the host is on-line.
    """
    whatismyip = 'http://bot.whatismyipaddress.com/'
    try:
        myip = urllib2.urlopen(whatismyip, timeout=5).readlines()[0]
    except:
        puts(red('Unable to derive IP through {0}'.format(whatismyip)))
        myip = '127.0.0.1'
    return myip


def home():
    return run('echo $HOME')


@task
# parallel does not work with this implementation. Needs to deal with the hosts
# individually.
#@parallel
def check_ssh(timeout=60.):
    """
    Check availability of SSH
    """
    each_timeout = 5.
    ntries = math.ceil(timeout / each_timeout)
    tries = 0

    sleep_time = 0
    while tries < ntries:
        time.sleep(sleep_time)
        start = time.time()
        try:
            with settings(timeout=each_timeout, warn_only=True):
                run('echo', quiet=True)
                puts(green("SSH is working!"))
            return
        except NetworkError:
            tries += 1
            puts(yellow("Cannot connect through SSH (%d/%d tries)" % (tries, ntries)))
            sleep_time = each_timeout - (time.time() - start)
            sleep_time = max(sleep_time, 0)

    error = "No SSH connection could be established to %s after %.2f seconds.\n" % (env.host, timeout)
    if is_localhost():
        error += ("Check that you have an SSH server installed and running.\n"
                  "In most Linux distributions this is ensured by installing "
                  "the openssh-server package.\n"
                  "In MacOS you enable this by "
                  "checking the 'Remote Login' preference in the 'Sharing' "
                  "preference panel.")
    else:
        error += ("Check that you are connecting to the correct host with the "
                  "correct user, and that an SSH server is running in the "
                  "remote machine")

    abort(error)


# Replacement functions for running commands
# They wrap up the command with useful things
def run(*args, **kwargs):
    with hide('running'):
        com = args[0]
        com = 'unset PYTHONPATH; {0}'.format(com)
        if 'quiet' not in kwargs or not kwargs['quiet']:
            puts('Executing: {0}'.format(com))
        if 'quiet' not in kwargs:
            kwargs['quiet'] = False
        res = frun(com, **kwargs)
    return res


def sudo(*args, **kwargs):
    with hide('running'):
        com = args[0]
        com = 'unset PYTHONPATH; {0}'.format(com)
        puts('Executing: {0}'.format(com))
    res = fsudo(com, quiet=True, **kwargs)
    return res


def is_localhost():
    # ensure something is run in that host
    if not env.host:
        run('echo')
    return env.host == 'localhost' or env.host.startswith("127.0.") or \
                       env.host == socket.gethostname()


def key_filename(key_name):
    return os.path.join(os.path.expanduser('~/.ssh/'), '{0}.pem'.format(key_name))


def get_public_key(key_filename):

    from Crypto.PublicKey import RSA

    with open(key_filename) as f:
        okey = RSA.importKey(f.read())
        return okey.exportKey('OpenSSH')


def generate_key_pair():

    from Crypto.PublicKey import RSA

    key = RSA.generate(2048)
    pubkey = key.publickey()
    return key, pubkey


def repo_root():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))


def _colored_puts(msg, color, with_stars):
    if with_stars:
        puts(color("******** %s ********" % (msg,)))
    else:
        puts(color(msg))


def success(msg, with_stars=True):
    _colored_puts(msg, green, with_stars)


def failure(msg, with_stars=True):
    _colored_puts(msg, red, with_stars)


def warning(msg, with_stars=True):
    _colored_puts(msg, yellow, with_stars)


def info(msg, with_stars=False):
    _colored_puts(msg, blue, with_stars)


def append_desc(t):
    tname = t.__name__
    desc = os.path.join(repo_root(), 'fabfile', 'doc', '%s_desc.rst' % (tname,)
                        )
    try:
        with open(desc, "rt") as f:
            t.__doc__ += "\n\n" + f.read()
    except IOError:
        t = ''
    return t
