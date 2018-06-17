from fabric.state import env

# import the port numbers used by daliuge to adjust security group
from dlg.manager.constants import DEFAULT_PORTS


# The following variable will define the Application name as well as directory
# structure and a number of other application specific names.
APP = 'daliuge'

# Define the correct version of python to be used.
APP_PYTHON_VERSION = '3.6'

# APP_PYTHON_URL = 'https://www.python.org/ftp/python/2.7.14/Python-2.7.14.tgz'

# AWS settings
DEFAULT_AWS_KEY_NAME = 'icrar_ngas'
DEFAULT_AWS_SEC_GROUP = APP.upper()  # Security group SSH and other ports

# Alpha-sorted packages per package manager
env.pkgs = {
            'YUM_PACKAGES': [
                     'python36',
                     'python36-devel',
                     'readline-devel',
                     'openssl-devel',
                     'gcc',
                     'docker',
                     'git',
                     ],
            'APT_PACKAGES': [
                    'python-dev',
                    'python-setuptools',
                    'tar',
                    'wget',
                    'gcc',
                    'docker',
                    'git',
                    ],
            'SLES_PACKAGES': [
                    'python-devel',
                    'wget',
                    'zlib',
                    'zlib-devel',
                    'gcc',
                    ],
            'BREW_PACKAGES': [
                    'wget',
                    ],
            'PORT_PACKAGES': [
                    'wget',
                    ],
            'APP_EXTRA_PYTHON_PACKAGES': [
                    'pycrypto',
                    'sphinx'
                    ],
        }

# NOTE: Make sure to modify the following lists to meet the requirements for
# the application.
APP_DATAFILES = [
]

# The username to use by default on remote hosts where APP is being installed
# This user might be different from the initial username used to connect to the
# remote host, in which case it will be created first
APP_USER = APP.lower()

# Name of the directory where APP sources will be expanded on the target host
# This is relative to the APP_USER home directory
APP_SRC_DIR_NAME = APP.lower() + '_src'

# Name of the directory where APP root directory will be created
# This is relative to the APP_USER home directory
APP_ROOT_DIR_NAME = APP.upper()

# Name of the directory where a virtualenv will be created to host the APP
# software installation, plus the installation of all its related software
# This is relative to the APP_USER home directory
APP_INSTALL_DIR_NAME = APP.lower() + '_rt'
