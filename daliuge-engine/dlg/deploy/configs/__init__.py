#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2015
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
import os, string

# ===================
# Deployment defaults
# ====================
ACCOUNT = ""
LOGIN_NODE = ""
HOME_DIR = os.environ["HOME"] if "HOME" in os.environ else ""
DLG_ROOT = f"{HOME_DIR}/dlg"
LOG_DIR = f"{DLG_ROOT}/log"
MODULES = ""
VENV = f"{DLG_ROOT}/venv"
DEFAULT_MON_HOST = "dlg-mon.icrar.org"  # TODO: need to get this running
DEFAULT_MON_PORT = 8898


__sub_tpl_str = """#!/bin/bash --login

#SBATCH --nodes=$NUM_NODES
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=2
#SBATCH --job-name=DALiuGE-$SESSION_ID
#SBATCH --time=$JOB_DURATION
#SBATCH --error=err-%j.log
$MODULES
export DLG_ROOT=$DLG_ROOT
$VENV

srun -l $PY_BIN -m dlg.deploy.start_dlg_cluster --log_dir $LOG_DIR $GRAPH_PAR $PROXY_PAR $GRAPH_VIS_PAR $LOGV_PAR $ZERORUN_PAR $MAXTHREADS_PAR $SNC_PAR $NUM_ISLANDS_PAR $ALL_NICS $CHECK_WITH_SESSION --ssid $SESSION_ID
"""
init_tpl = string.Template(__sub_tpl_str)


class DefaultConfig(object):

    def __init__(self):
        self._dict = dict()
        self.setpar("host", self.LOGIN_NODE)
        self.setpar("account", self.ACCOUNT)
        self.setpar("home_dir", self.HOME_DIR.strip())
        self.setpar("dlg_root", self.DLG_ROOT.strip())
        self.setpar("log_root", self.LOG_DIR)
        self.setpar("modules", self.MODULES.strip())
        self.setpar("venv", self.VENV.strip())

    def setpar(self, k, v):
        self._dict[k] = v

    def getpar(self, k):
        return self._dict.get(k)


#############################


class ICRARoodConfig(DefaultConfig):
    MODULES = """
    module load python/3.8.12
    """
    # The following is more a workaround than a solution
    # requires the user to have a venv exectly in that place
    ACCOUNT = os.environ["USER"]
    HOME_DIR = os.environ["HOME"] if "HOME" in os.environ else ""
    DLG_ROOT = f"{HOME_DIR}/dlg"
    LOG_DIR = f"{DLG_ROOT}/log"
    VENV = f"source {HOME_DIR}/dlg/venv/bin/activate"

    def __init__(self):
        super(ICRARoodConfig, self).__init__()

    def init_list(self):  # TODO please fill in
        return [self.ACCOUNT, self.LOG_DIR, self.MODULES, self.VENV]


class ICRARoodCldConfig(DefaultConfig):
    # The following is more a workaround than a solution
    # requires the user to have a venv exectly in that place
    ACCOUNT = os.environ["USER"]
    HOME_DIR = os.environ["HOME"]
    DLG_ROOT = f"{HOME_DIR}/dlg"
    LOG_DIR = f"{DLG_ROOT}/log"
    # The compute nodes have have required python and DALiuGE but just in case....
    VENV = f"source {DLG_ROOT}/venv/bin/activate"

    def __init__(self):
        super(ICRARoodCldConfig, self).__init__()

    def init_list(self):  # TODO please fill in
        return [self.ACCOUNT, self.LOG_DIR, self.VENV]


class GalaxyMWAConfig(DefaultConfig):
    def __init__(self):
        super(GalaxyMWAConfig, self).__init__()

    def init_list(self):
        return ["mwaops", "/group/mwaops/cwu/dfms/logs"]


class GalaxyASKAPConfig(DefaultConfig):
    MODULES = """
module swap PrgEnv-cray PrgEnv-gnu
module load python/2.7.10
module load mpi4py
"""
    VENV = ""

    def __init__(self):
        super(GalaxyASKAPConfig, self).__init__()

    def init_list(self):
        return [
            "astronomy856",
            "/group/astronomy856/cwu/dfms/logs",
            self.MODULES,
        ]


class MagnusConfig(DefaultConfig):
    def __init__(self):
        super(MagnusConfig, self).__init__()

    def init_list(self):
        return ["pawsey0129", "/group/pawsey0129/daliuge_logs"]


class Setonix411Config(DefaultConfig):
    """
    Configuration for project 0411 on Setonix.
    """

    LOGIN_NODE = "setonix.pawsey.org.au"
    ACCOUNT = "pawsey0411"
    USER = os.environ["USER"] if "USER" in os.environ else ""
    HOME_DIR = f"/scratch/{ACCOUNT}"
    DLG_ROOT = f"{HOME_DIR}/{USER}/dlg"
    LOG_DIR = f"{DLG_ROOT}/log"
    MODULES = ""
    VENV = f"source /software/projects/{ACCOUNT}/venv/bin/activate"

    MODULES = ""

    def __init__(self):
        super(Setonix411Config, self).__init__()

    def init_list(self):
        return [self.ACCOUNT, f"{self.HOME_DIR}/logs"]


class TianHe2Config(DefaultConfig):
    def __init__(self):
        super(TianHe2Config, self).__init__()

    def init_list(self):  # TODO please fill in
        return ["SHAO", "/group/shao/daliuge_logs"]


##########################################


class ConfigFactory:
    mapping = {
        "galaxy_mwa": GalaxyMWAConfig,
        "galaxy_askap": GalaxyASKAPConfig,
        "magnus": MagnusConfig,
        "galaxy": GalaxyASKAPConfig,
        "setonix": Setonix411Config,
        "shao": TianHe2Config,
        "hyades.icrar.org": ICRARoodConfig,
        "ood_cloud": ICRARoodCldConfig,
    }

    @staticmethod
    def available():
        return list(ConfigFactory.mapping.keys())

    @staticmethod
    def create_config(facility=None):
        facility = facility.lower() if (facility is not None) else facility
        return ConfigFactory.mapping.get(facility)()
