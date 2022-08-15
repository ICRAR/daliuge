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

import os
import shutil
import tempfile
import urllib.parse
import unittest

import pkg_resources
from dlg import common
from dlg.common import tool
from dlg.restutils import RestClient, RestClientException

lg_dir = pkg_resources.resource_filename(__name__, ".")  # @UndefinedVariable
lgweb_port = 8086


class TestLGWeb(unittest.TestCase):
    def setUp(self):
        unittest.TestCase.setUp(self)
        self.temp_dir = tempfile.mkdtemp()
        args = [
            "-d",
            lg_dir,
            "-t",
            self.temp_dir,
            "-p",
            str(lgweb_port),
            "-H",
            "localhost",
        ]
        self.devnull = open(os.devnull, "wb")
        self.web_proc = tool.start_process(
            "lgweb", args, stdout=self.devnull, stderr=self.devnull
        )

    def tearDown(self):
        shutil.rmtree(self.temp_dir)
        self.devnull.close()
        common.terminate_or_kill(self.web_proc, 10)
        unittest.TestCase.tearDown(self)

    def _generate_pgt(self, c):
        c._GET(
            "/gen_pgt?lg_name=logical_graphs/chiles_simple.graph&num_par=5&algo=metis&min_goal=0&ptype=0&max_load_imb=100"
        )

    def test_get_lgjson(self):

        c = RestClient("localhost", lgweb_port, timeout=10)

        # a specific one
        lg = c._get_json("/jsonbody?lg_name=logical_graphs/chiles_simple.graph")
        self.assertIsNotNone(lg)

        # by default the first one found by the lg_web should be returned
        lg = c._get_json("/jsonbody")
        self.assertIsNotNone(lg)

        # doesn't exist
        self.assertRaises(
            RestClientException, c._get_json, "/jsonbody?lg_name=doesnt_exist.graph"
        )

    def test_post_lgjson(self):

        c = RestClient("localhost", lgweb_port, timeout=10)

        # new graphs cannot currently be added
        form_data = {
            "lg_name": "new.graph",
            "lg_content": '{"id": 1, "name": "example"}',
            "rmode": "1",
        }
        self.assertRaises(RestClientException, c._post_form, "/jsonbody", form_data)

        # Replace the contents of an existing one
        # (but replace it back with original after the test)
        original_fname = os.path.join(lg_dir, "logical_graphs", "chiles_simple.graph")
        copy_fname = tempfile.mktemp()
        shutil.copy(original_fname, copy_fname)

        try:
            form_data["lg_name"] = "logical_graphs/chiles_simple.graph"
            c._post_form("/jsonbody", form_data)
            new = c._get_json("/jsonbody?lg_name=logical_graphs/chiles_simple.graph")
            self.assertIsNotNone(new)
            self.assertIn("id", new)
            self.assertIn("name", new)
            self.assertEqual(1, new["id"])
            self.assertEqual("example", new["name"])
            self.assertEqual("1", new["reprodata"]["rmode"])
        finally:
            shutil.move(copy_fname, original_fname)

    def test_gen_pgt(self):

        c = RestClient("127.0.0.1", lgweb_port, timeout=10)

        # doesn't exist!
        self.assertRaises(
            RestClientException,
            c._GET,
            "/gen_pgt?lg_name=doesnt_exist.json&num_par=5&algo=metis&min_goal=0&ptype=0&max_load_imb=100",
        )
        # unknown algorithm
        self.assertRaises(
            RestClientException,
            c._GET,
            "/gen_pgt?lg_name=logical_graphs/chiles_simple.graph&num_par=5&algo=noidea",
        )
        # this should work now
        self._generate_pgt(c)

    def test_get_pgtjson(self):

        c = RestClient("localhost", lgweb_port, timeout=10)
        c._GET(
            "/gen_pgt?lg_name=logical_graphs/chiles_simple.graph&num_par=5&algo=metis&min_goal=0&ptype=0&max_load_imb=100"
        )

        # doesn't exist
        self.assertRaises(
            RestClientException, c._get_json, "/pgt_jsonbody?pgt_name=unknown.json"
        )
        # good!
        c._get_json("/pgt_jsonbody?pgt_name=logical_graphs/chiles_simple1_pgt.graph")

    def test_get_pgt_post(self):

        c = RestClient("localhost", lgweb_port, timeout=10)

        # an API call with an empty form should cause an error
        self.assertRaises(RestClientException, c._POST, "/gen_pgt")

        # new logical graph JSON
        fname = os.path.join(lg_dir, "logical_graphs", "test-20190830-110556.graph")
        with open(fname, "rb") as infile:
            json_data = infile.read()

        # add 'correct' data to the form
        form_data = {
            "algo": "metis",
            "lg_name": "metis.graph",
            "json_data": json_data,
            "num_islands": 0,
            "num_par": 1,
            "par_label": "Partition",
            "max_load_imb": 100,
            "max_cpu": 8,
        }

        # POST form to /gen_pgt
        try:
            content = urllib.parse.urlencode(form_data)
            c._POST(
                "/gen_pgt", content, content_type="application/x-www-form-urlencoded"
            )
        except RestClientException as e:
            self.fail(e)

    def test_mkn_pgt_post(self):

        c = RestClient("localhost", lgweb_port, timeout=10)

        # an API call with an empty form should cause an error
        self.assertRaises(RestClientException, c._POST, "/gen_pgt")

        # new logical graph JSON
        fname = os.path.join(lg_dir, "logical_graphs", "simpleMKN.graph")
        with open(fname, "rb") as infile:
            js = infile.read()

        # add 'correct' data to the form
        form_data = {
            "algo": "metis",
            "lg_name": "metis.graph",
            "num_islands": 0,
            "num_par": 1,
            "par_label": "Partition",
            "max_load_imb": 100,
            "max_cpu": 8,
            "json_data": js,
        }

        # POST form to /gen_pgt
        try:
            content = urllib.parse.urlencode(form_data)
            c._POST(
                "/gen_pgt", content, content_type="application/x-www-form-urlencoded"
            )
        except RestClientException as e:
            self.fail(e)

    def test_loop_pgt_post(self):

        c = RestClient("localhost", lgweb_port, timeout=10)

        # an API call with an empty form should cause an error
        self.assertRaises(RestClientException, c._POST, "/gen_pgt")

        # new logical graph JSON
        with open(
            os.path.join(lg_dir, "logical_graphs", "testLoop.graph"), "rb"
        ) as infile:
            json_data = infile.read()

        # add 'correct' data to the form
        form_data = {
            "algo": "metis",
            "lg_name": "metis.graph",
            "json_data": json_data,
            "num_islands": 0,
            "num_par": 1,
            "par_label": "Partition",
            "max_load_imb": 100,
            "max_cpu": 8,
        }

        # POST form to /gen_pgt
        try:
            content = urllib.parse.urlencode(form_data)
            c._POST(
                "/gen_pgt", content, content_type="application/x-www-form-urlencoded"
            )
        except RestClientException as e:
            self.fail(e)

    def test_pg_viewerer(self):

        c = RestClient("localhost", lgweb_port, timeout=10)
        self._generate_pgt(c)

        # doesn't exist
        self.assertRaises(
            RestClientException, c._GET, "/pg_viewer?pgt_view_name=unknown.json"
        )
        # Defaults to first PGT
        c._GET("/pg_viewer")
        # also fine, PGT exists
        c._GET("/pg_viewer?pgt_view_name=logical_graphs/chiles_simple1_pgt.graph")

    def _test_pgt_action(self, path, unknown_fails):

        c = RestClient("localhost", lgweb_port, timeout=10)
        self._generate_pgt(c)

        # doesn't exist
        if unknown_fails:
            self.assertRaises(
                RestClientException, c._GET, "/" + path + "?pgt_id=unknown.json"
            )
        else:
            c._GET("/" + path + "?pgt_id=unknown.json")

        # exists
        c._GET("/" + path + "?pgt_id=logical_graphs/chiles_simple1_pgt.graph")

    def test_show_gantt_chart(self):
        self._test_pgt_action("show_gantt_chart", False)

    def test_show_schedule_mat(self):
        self._test_pgt_action("show_schedule_mat", False)

    def test_get_gantt_chart(self):
        self._test_pgt_action("pgt_gantt_chart", True)
