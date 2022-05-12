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

__sub_tpl_str = """#!/bin/bash --login

#SBATCH --nodes=$NUM_NODES
#SBATCH --ntasks-per-node=1
#SBATCH --job-name=DALiuGE-$SESSION_ID
#SBATCH --time=$JOB_DURATION
#SBATCH --error=err-%j.log
$MODULES
$VENV

srun -l $PY_BIN -m dlg.deploy.start_dlg_cluster -l $LOG_DIR $GRAPH_PAR $PROXY_PAR $GRAPH_VIS_PAR $LOGV_PAR $ZERORUN_PAR $MAXTHREADS_PAR $SNC_PAR $NUM_ISLANDS_PAR $ALL_NICS $CHECK_WITH_SESSION --ssid $SESSION_ID --remote-mechanism slurm
"""
init_tpl = string.Template(__sub_tpl_str)


class DefaultConfig(object):
    MODULES = ""
    VENV = ""

    def __init__(self):
        self._dict = dict()
        l = self.init_list()
        self.setpar("acc", l[0])
        self.setpar("log_root", l[1])
        self.setpar("modules", l[2].strip())
        self.setpar("venv", l[3].strip())

    def init_list(self):
        pass

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
    HOME_DIR = os.environ["HOME"]
    LOG_DIR = f"{HOME_DIR}/dlg/runs"
    VENV = f"source {HOME_DIR}/dlg/venv/bin/activate"

    def __init__(self):
        super(ICRARoodConfig, self).__init__()

    def init_list(self):  # TODO please fill in
        return [self.ACCOUNT, self.LOG_DIR, self.MODULES, self.VENV]


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
        return ["astronomy856", "/group/astronomy856/cwu/dfms/logs", self.MODULES]


class MagnusConfig(DefaultConfig):
    def __init__(self):
        super(MagnusConfig, self).__init__()

    def init_list(self):
        return ["pawsey0129", "/group/pawsey0129/daliuge_logs"]


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
        "shao": TianHe2Config,
        "hyades.icrar.org": ICRARoodConfig,
    }

    @staticmethod
    def available():
        return list(ConfigFactory.mapping.keys())

    @staticmethod
    def create_config(facility=None):
        facility = facility.lower() if (facility is not None) else facility
        return ConfigFactory.mapping.get(facility)()
