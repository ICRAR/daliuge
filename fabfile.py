"""
Fabric file for building installing and testing Python projects
    
Authors: A Wicenec and D Pallot
    
ICRAR 2015
"""
import os
import time, urllib

from fabric.api import run, sudo, put, env, require, local, task
from fabric.context_managers import cd, hide, settings
from fabric.contrib.console import confirm
from fabric.contrib.files import append, sed, comment, exists
from fabric.decorators import task, serial
from fabric.operations import prompt
from fabric.utils import puts, abort, fastprint


APP_PYTHON_VERSION = '2.7'
APP_PYTHON_URL = 'https://www.python.org/ftp/python/2.7.8/Python-2.7.8.tgz'
VIRTUALENV_URL = 'https://pypi.python.org/packages/source/v/virtualenv/virtualenv-12.0.7.tar.gz'


VENV_DIR = 'projects' # virtual env directory relative to current user home
PROJECT = 'dfms' # project name


def set_env():
    env.HOME = run("echo ~")
    env.APP_DIR_ABS = "{0}/{1}/{2}".format(env.HOME, VENV_DIR, PROJECT)


def to_boolean(choice, default=False):
    """Convert the yes/no to true/false
        
        :param choice: the text string input
        :type choice: string
        """
    valid = {"yes":True,   "y":True,  "ye":True,
        "no":False,     "n":False}
    choice_lower = choice.lower()
    if choice_lower in valid:
        return valid[choice_lower]
    return default


def check_command(command):
    """
        Check existence of command remotely
        
        INPUT:
        command:  string
        
        OUTPUT:
        Boolean
        """
    res = run('if command -v {0} &> /dev/null ;then command -v {0};else echo ;fi'.format(command))
    return res


def check_dir(directory):
    """
        Check existence of remote directory
        """
    res = run('if [ -d {0} ]; then echo 1; else echo ; fi'.format(directory))
    return res


def check_path(path):
    """
        Check existence of remote path
        """
    res = run('if [ -e {0} ]; then echo 1; else echo ; fi'.format(path))
    return res


def check_python():
    """
        Check for the existence of correct version of python
        
        INPUT:
        None
        
        OUTPUT:
        path to python binary    string, could be empty string
        """
    # Try whether there is already a local python installation for this user
    ppath = os.path.realpath(env.APP_DIR_ABS+'/../python')
    ppath = check_command('{0}/bin/python{1}'.format(ppath, APP_PYTHON_VERSION))
    if ppath:
        return ppath
    # Try python2.7 first
    ppath = check_command('python{0}'.format(APP_PYTHON_VERSION))
    if ppath:
        env.PYTHON = ppath
        return ppath


def virtualenv(command):
    """
        Just a helper function to execute commands in the virtualenv
        """
    
    env.activate = 'source {0}/bin/activate'.format(env.APP_DIR_ABS)
    with cd(env.APP_DIR_ABS):
        run(env.activate + ' && ' + command)


@task
def python_setup():
    """
        Ensure that there is the right version of python available
        If not install it from scratch in user directory.
        
        INPUT:
        None
        
        OUTPUT:
        None
        """
    set_env()
    
    with cd('/tmp'):
        run('wget --no-check-certificate -q {0}'.format(APP_PYTHON_URL))
        base = os.path.basename(APP_PYTHON_URL)
        pdir = os.path.splitext(base)[0]
        run('tar -xzf {0}'.format(base))
    ppath = run('echo $PWD') + '/python'
    with cd('/tmp/{0}'.format(pdir)):
        #run('./configure --prefix {0};make;make install'.format(ppath))
        run('./configure --prefix {0};make'.format(ppath))
        ppath = '{0}/bin/python{1}'.format(ppath,APP_PYTHON_VERSION)
    env.PYTHON = ppath


@task
def virtualenv_setup():
    """
        setup virtualenv with the detected or newly installed python
        """
    
    set_env()
    
    ppath = check_python()
    if not ppath:
        python_setup()
    else:
        env.PYTHON = ppath
    
    if not check_dir(env.APP_DIR_ABS):
        with cd('/tmp'):
            print "### CREATING VIRTUAL ENV ###"
            run('wget {0}'.format(VIRTUALENV_URL))
            vbase = VIRTUALENV_URL.split('/')[-1]
            run('tar -xzf {0}'.format(vbase))
            run('cd {0}; {1} virtualenv.py {2}'.format(vbase.split('.tar.gz')[0],
                                                       env.PYTHON, env.APP_DIR_ABS))
            run('rm -rf virtualenv*')


def install_egg():
    reploc = os.path.dirname(os.path.abspath(__file__))
    virtualenv('easy_install {0}/dist/*.egg'.format(reploc))



def uninstall_egg():
    virtualenv('/usr/bin/yes | pip uninstall {0}'.format(PROJECT))


def build_egg():
    reploc = os.path.dirname(os.path.abspath(__file__))
    virtualenv('cd {0}; python {0}/setup.py bdist_egg'.format(reploc))


def invoke_tests():
    reploc = os.path.dirname(os.path.abspath(__file__))
    virtualenv('cd {0}; python {0}/setup.py test'.format(reploc))


@task
def build():
    """
        build package binary as an egg
        """
    set_env()
    virtualenv_setup()
    print "### BUILDING ###"
    build_egg()


@task
def build_install():
    """
        build package binary as an egg and install within virtual environment
        """
    build()
    print "### INSTALLING ###"
    install_egg()


@task
def uninstall():
    """
        uninstall package egg from virtual environment
        """
    print "### UNINSTALLING ###"
    set_env()
    virtualenv_setup()
    uninstall_egg()


@task
def run_tests():
    """
        build, install and run tests within virtual environment
        """
    build_install()
    
    print "### RUNNING TESTS ###"
    invoke_tests()


@task
def virtualenv_clean():
    """
        remove virtualenv
        """
    set_env()
    
    print "### REMOVING VIRTUAL ENV ###"
    
    run('rm -rf {0}'.format(env.APP_DIR_ABS))

