#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2024
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

import unittest
import pytest
from pathlib import Path
# Note this test will only run with a full installation of DALiuGE.
pexpect = pytest.importorskip("dlg.dropmake.pg_generator")

try:
    from importlib.resources import files
except ModuleNotFoundError:
    from importlib_resources import files # type: ignore
import dlg.deploy.configs as configs
import daliuge_tests.engine.graphs as test_graphs


from dlg.deploy.slurm_client import SlurmClient
import json 

class TestSlurmClient(unittest.TestCase):

    def test_client_with_cli(self):
        # Use special graph that also contains file name. See 'create_dlg_job.py'
        pg = files(test_graphs) / "SLURM_HelloWorld_simplePG.graph"
          
        client = SlurmClient(
            facility="setonix", 
            num_nodes=6,
            job_dur=45,
            physical_graph_template_file=str(pg),
            suffix="TestSession",
            username="test"
        )
        job_desc = client.create_job_desc(pg)
        curr_file = Path(__file__)
        compare_script = curr_file.parent / "slurm_script.sh"
        with compare_script.open() as fp:
            script = fp.read()
            self.assertEqual(script, job_desc)
        
    def test_client_with_configfile(self):
        """
        Using the INI file, test:
        - That we produce the same as the CLI with the same parameters
        - That we can use the INI file to produce alternative parameters
        """
        from dlg.deploy.create_dlg_job import process_config
        pg = files(test_graphs) / "SLURM_HelloWorld_simplePG.graph"
        cfg_file = Path(__file__).parent / "setonix.ini"
        cfg = process_config(cfg_file)
        client = SlurmClient(
            facility="setonix", 
            num_nodes=6,
            job_dur=45,
            physical_graph_template_file=str(pg),
            suffix="TestSession",
            config=cfg,
            username='test'
        )

        job_desc = client.create_job_desc(pg)
        curr_file = Path(__file__)
        compare_script = curr_file.parent / "slurm_script.sh"
        with open('slurm_script.sh', 'w') as fp: 
            fp.write(job_desc)
        with compare_script.open() as fp:
            script = fp.read()
            self.assertEqual(script, job_desc)


    def test_client_with_slurm_template(self):
        """
        Use 'slurm_script_from_template.sh as a comparison file to demonstrate
        how the template approach gives us more options. 
        """
        pg = files(test_graphs) / "SLURM_HelloWorld_simplePG.graph"
        template = Path(__file__).parent / "example_template.slurm"
        with template.open() as fp:
            slurm_template = fp.read()
        client = SlurmClient(
            facility="setonix", 
            physical_graph_template_file=str(pg),
            suffix="TestSession",
            slurm_template=slurm_template,
            username='test'
        )
        job_desc = client.create_job_desc(pg)
        curr_file = Path(__file__)
        compare_script = curr_file.parent / "slurm_script_from_template.sh"
        with open('output.sh', 'w') as fp: 
            fp.write(job_desc)
        with compare_script.open() as fp:
            script = fp.read()
            self.assertEqual(script, job_desc)
