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

import json
import os
import shutil
import tempfile
import unittest
from parameterized import parameterized
import urllib.parse
import logging

from dlg import common
from dlg.common import tool
from dlg.dropmake.web.translator_utils import get_mgr_deployment_methods
from dlg.restutils import RestClient, RestClientException

from importlib.resources import files

import daliuge_tests.dropmake as test_graphs

lg_dir = files(test_graphs)
lgweb_port = 8086
logger = logging.getLogger(f"dlg.{__name__}")


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
            "-vv",
            # "-l",
            # self.temp_dir,
        ]
        # uncomment to capture local logs, but reverse before
        # push to github.
        # lf = f"{self.temp_dir}/dlgTrans.log"
        lf = "/dev/null"
        with open(lf, "wb") as self.logfile:
            self.web_proc = tool.start_process(
                "lgweb", args, stdout=self.logfile, stderr=self.logfile
            )

    def tearDown(self):
        if self.logfile.name == "/dev/null":
            shutil.rmtree(self.temp_dir)
        else:
            logger.info("Kept logfile in: %s", self.logfile.name)
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
            RestClientException,
            c._get_json,
            "/jsonbody?lg_name=doesnt_exist.graph",
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
        c = RestClient("localhost", lgweb_port, timeout=10)

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
            RestClientException,
            c._get_json,
            "/pgt_jsonbody?pgt_name=unknown.json",
        )
        # good!
        c._get_json("/pgt_jsonbody?pgt_name=logical_graphs/chiles_simple1_pgt.graph")

    def test_get_pgt_post(self, algo="metis", algo_options=None):
        c = RestClient("localhost", lgweb_port, timeout=10)

        # an API call with an empty form should cause an error
        self.assertRaises(RestClientException, c.POST, "/gen_pgt")

        # new logical graph JSON
        fname = os.path.join(lg_dir, "logical_graphs", "test-20190830-110556.graph")
        with open(fname, "rb") as infile:
            json_data = infile.read()

        # add 'correct' data to the form
        form_data = {
            "algo": algo,
            "lg_name": "metis.graph",
            "json_data": json_data,
            "num_islands": 0,
            "num_par": 1,
            "par_label": "Partition",
            "max_load_imb": 100,
            "max_cpu": 8,
        }
        if algo_options is not None:
            form_data.update(algo_options)

        # POST form to /gen_pgt
        try:
            content = urllib.parse.urlencode(form_data)
            c.POST(
                "/gen_pgt",
                content,
                content_type="application/x-www-form-urlencoded",
            )
        except RestClientException as e:
            self.fail(e)

    @unittest.skip("MKN does not work at this point")
    def test_mkn_pgt_post(self):
        c = RestClient("localhost", lgweb_port, timeout=10)

        # an API call with an empty form should cause an error
        self.assertRaises(RestClientException, c.POST, "/gen_pgt")

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
            c.POST(
                "/gen_pgt",
                content,
                content_type="application/x-www-form-urlencoded",
            )
        except RestClientException as e:
            self.fail(e)

    def test_loop_pgt_post(self):
        c = RestClient("localhost", lgweb_port, timeout=10)

        # an API call with an empty form should cause an error
        self.assertRaises(RestClientException, c.POST, "/gen_pgt")

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
            c.POST(
                "/gen_pgt",
                content,
                content_type="application/x-www-form-urlencoded",
            )
        except RestClientException as e:
            self.fail(e)

    @unittest.skip("None translation is not an option in EAGLE and does not work.")
    def test_none_translation(self):
        self.test_get_pgt_post(algo="none")

    def test_metis_translation(self):
        self.test_get_pgt_post(algo="metis")

    def test_sarkar_translation(self):
        self.test_get_pgt_post(algo="mysarkar")

    def test_min_num_parts_translation(self):
        self.test_get_pgt_post(
            algo="min_num_parts",
            algo_options={"deadline": 300, "time_greedy": 50},
        )

    @unittest.skip("This one fails sometimes with HTTP error 557.")
    def test_pso_translation(self):
        self.test_get_pgt_post(
            algo="pso", algo_options={"swarm_size": 10, "deadline": 300}
        )

    def test_pg_viewer(self):
        c = RestClient("localhost", lgweb_port, timeout=10)
        self._generate_pgt(c)

        # doesn't exist
        self.assertRaises(
            RestClientException,
            c._GET,
            "/pg_viewer?pgt_view_name=unknown.json",
        )
        # Defaults to first PGT
        c._GET("/pg_viewer")
        # also fine, PGT exists
        # c._GET("/pg_viewer?pgt_view_name=logical_graphs/chiles_simple2_pgt.graph")

    def _test_pgt_action(self, path, unknown_fails):
        c = RestClient("localhost", lgweb_port, timeout=10)
        self._generate_pgt(c)

        # doesn't exist
        if unknown_fails:
            self.assertRaises(
                RestClientException,
                c._GET,
                "/" + path + "?pgt_id=unknown.json",
            )
        else:
            c._GET("/" + path + "?pgt_id=unknown.json")

        # exists
        c._GET("/" + path + "?pgt_id=logical_graphs/chiles_simple1_pgt.graph")

    def test_show_gantt_chart(self):
        self._test_pgt_action("show_gantt_chart", False)

    def test_show_schedule_mat(self):
        self._test_pgt_action("show_schedule_mat", False)

    @unittest.skip("This one fails on github, but not else.")
    def test_get_gantt_chart(self):
        self._test_pgt_action("pgt_gantt_chart", True)

    def test_get_submission_methods(self):
        import json

        c = RestClient("localhost", lgweb_port, timeout=10)
        response = c._GET("/api/submission_method")
        response_content = json.load(response)
        self.assertEqual(response_content, {"methods": []})

    def _test_post_request(
        self,
        client: RestClient,
        url: str,
        form_data: dict = None,
        expect_fail=True,
    ):
        if form_data:
            content = urllib.parse.urlencode(form_data)
        else:
            content = None
        if expect_fail:
            if content:
                self.assertRaises(
                    RestClientException,
                    client.POST,
                    url,
                    content,
                    content_type="application/x-www-form-urlencoded",
                )
            else:
                self.assertRaises(RestClientException, client.POST, url)
        else:
            if content:
                try:
                    ret = client.POST(
                        url,
                        content,
                        content_type="application/x-www-form-urlencoded",
                    )
                except RestClientException as e:
                    self.fail(e)
            else:
                try:
                    ret = client.POST(url)
                except RestClientException as e:
                    self.fail(e)
            return json.load(ret)

    @parameterized.expand([("0", "testLoop.graph"), ("1", "ArrayLoop.graph")])
    def test_get_fill(self, n, graph):
        c = RestClient("localhost", lgweb_port, timeout=10)
        test_url = "/lg_fill"
        with open(os.path.join(lg_dir, "logical_graphs", graph), "rb") as infile:
            json_data = infile.read()
            logger.info("Logical graph %s loaded", infile.name)
        request_tests = [
            (None, True),  # Call with an empty form should cause an error
            ({"lg_name": "metis.graph"}, True),  # Invalid lg_name
            (
                {"lg_name": "logical_graphs/chiles_simple.graph"},
                False,
            ),  # Valid lg_name
            (
                {"lg_name": graph, "lg_content": json_data},
                True,
            ),  # Both lg_name and lg_content
            ({"lg_content": "{'garbage: 3}"}, True),  # Invalid lg_content
            ({"lg_content": json_data}, False),  # Valid lg_content
            (
                {"lg_content": json_data, "parameters": '{"nonsense: 3}'},
                True,
            ),  # Invalid parameters
            (
                {"lg_content": json_data, "parameters": '{"nonsense": 3}'},
                False,
            ),  # Valid parameters
        ]

        for request in request_tests:
            self._test_post_request(c, test_url, request[0], request[1])

    @parameterized.expand(
        [
            ("testLoop", "testLoop.graph"),
            ("ArrayLoop", "ArrayLoop.graph"),
        ]
    )
    def test_lg_unroll(self, n, graph):
        c = RestClient("localhost", lgweb_port, timeout=10)
        test_url = "/unroll"
        with open(os.path.join(lg_dir, "logical_graphs", graph), "rb") as infile:
            json_data = infile.read()

        request_tests = [
            (
                None,
                True,
            ),  # Call with an empty form should cause an error
            ({"lg_name": "metis.graph"}, True),  # Invalid lg_name
            (
                {"lg_name": "logical_graphs/chiles_simple.graph"},
                False,
            ),  # Valid lg_name
            (
                {"lg_name": graph, "lg_content": json_data},
                True,
            ),  # Both lg_name and lg_content
            (
                {"lg_content": "{'garbage: 3}"},
                True,
            ),  # Invalid lg_content
            ({"lg_content": json_data}, False),  # Valid lg_content
        ]

        for request in request_tests:
            self._test_post_request(c, test_url, request[0], request[1])

        # test default_app
        form_data = {
            "lg_content": json_data,
            "default_app": "dlg.data.drops.file.FileDROP",
        }
        pgt = self._test_post_request(c, test_url, form_data, False)
        for dropspec in pgt:
            if "dropclass" in dropspec and dropspec["category"] == "Application":
                self.assertEqual(dropspec["dropclass"], "test.app")

    def test_pgt_partition(self):
        c = RestClient("localhost", lgweb_port, timeout=10)
        test_url = "/partition"
        with open(
            os.path.join(lg_dir, "logical_graphs", "testLoop.graph"), "rb"
        ) as infile:
            json_data = infile.read()

        # Translate graph
        form_data = {"lg_content": json_data}
        pgt = self._test_post_request(c, "/unroll", form_data, False)
        pgt = json.dumps(pgt)

        request_tests = [
            (None, True),  # Call with an empty form should cause an error
            ({"pgt_content": pgt}, False),  # Simple partition
            (
                {"pgt_content": pgt, "num_partitions": 1, "num_islands": 3},
                True,
            ),  # num_partitions < num_islands
        ]

        for request in request_tests:
            self._test_post_request(c, test_url, request[0], request[1])

    def test_lg_unroll_and_partition(self):
        c = RestClient("localhost", lgweb_port, timeout=10)
        test_url = "/unroll_and_partition"
        with open(
            os.path.join(lg_dir, "logical_graphs", "testLoop.graph"), "rb"
        ) as infile:
            json_data = infile.read()

        request_tests = [
            (None, True),  # Call with an empty form should cause an error
            ({"lg_name": "fake.graph"}, True),  # Invalid lg_name
            (
                {"lg_name": "logical_graphs/chiles_simple.graph"},
                False,
            ),  # Valid lg_name
            (
                {"lg_name": "chiles_simple.graph", "lg_content": json_data},
                True,
            ),  # Both lg_name and lg_content
            ({"lg_content": "{'garbage: 3}"}, True),  # Invalid lg_content
            ({"lg_content": json_data}, False),  # Valid lg_content
            (
                {
                    "lg_content": json_data,
                    "num_partitions": 1,
                    "num_islands": 3,
                },
                True,
            ),  # num_partitions < num_islands
        ]

        for request in request_tests:
            self._test_post_request(c, test_url, request[0], request[1])

    def test_pgt_map(self):
        c = RestClient("localhost", lgweb_port, timeout=10)
        test_url = "/map"
        with open(
            os.path.join(lg_dir, "logical_graphs", "testLoop.graph"), "rb"
        ) as infile:
            json_data = infile.read()

        # unroll and partition graph
        pgt = self._test_post_request(
            c, "/unroll_and_partition", {"lg_content": json_data}, False
        )
        pgt = json.dumps(pgt)

        request_tests = [
            (None, True),  # Call with an empty form should cause an error
            (
                {
                    "pgt_content": pgt,
                    "nodes": "localhost",
                    "num_islands": 1,
                    "co_host_dim": True,
                },
                False,
            ),  # Simple partition
        ]

        for request in request_tests:
            self._test_post_request(c, test_url, request[0], request[1])

    def test_get_mgr_deployment_methods(self):
        response = get_mgr_deployment_methods("localhost", lgweb_port, "")
        self.assertEqual([], response)
