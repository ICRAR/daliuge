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
import unittest

import pkg_resources

import six.moves.urllib_parse as urllib  # @UnresolvedImport

from dlg import tool, utils
from dlg.restutils import RestClient, RestClientException

lg_dir = pkg_resources.resource_filename(__name__, '.')  # @UndefinedVariable
lgweb_port = 8000

class TestLGWeb(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)
        self.temp_dir = tempfile.mkdtemp()
        args = ['-d', lg_dir, '-t', self.temp_dir, '-p', str(lgweb_port), '-H', 'localhost']
        self.devnull = open(os.devnull, 'wb')
        self.web_proc = tool.start_process('lgweb', args, stdout=self.devnull, stderr=self.devnull)

    def tearDown(self):
        shutil.rmtree(self.temp_dir)
        self.devnull.close()
        utils.terminate_or_kill(self.web_proc, 10)
        unittest.TestCase.tearDown(self)

    def _generate_pgt(self, c):
        c._GET('/gen_pgt?lg_name=logical_graphs/chiles_simple.json&num_par=5&algo=metis&min_goal=0&ptype=0&max_load_imb=100')

    def test_get_lgjson(self):

        c = RestClient('localhost', lgweb_port, 10)

        # a specific one
        lg = c._get_json('/jsonbody?lg_name=logical_graphs/chiles_simple.json')
        self.assertIsNotNone(lg)

        # by default the first one found by the lg_web should be returned
        lg = c._get_json('/jsonbody')
        self.assertIsNotNone(lg)

        # doesn't exist
        self.assertRaises(RestClientException, c._get_json, '/jsonbody?lg_name=doesnt_exist.json')

    def test_post_lgjson(self):

        c = RestClient('localhost', lgweb_port, 10)

        # new graphs cannot currently be added
        form_data = {'lg_name': 'new.json', 'lg_content': '{"id": 1, "name": "example"}'}
        self.assertRaises(RestClientException, c._post_form, '/jsonbody', form_data)

        # Replace the contents of an existing one
        # (but replace it back with original after the test)
        original_fname = os.path.join(lg_dir, 'logical_graphs', 'chiles_simple.json')
        copy_fname = tempfile.mktemp()
        shutil.copy(original_fname, copy_fname)

        try:
            form_data['lg_name'] = 'logical_graphs/chiles_simple.json'
            c._post_form('/jsonbody', form_data)
            new = c._get_json('/jsonbody?lg_name=logical_graphs/chiles_simple.json')
            self.assertIsNotNone(new)
            self.assertIn('id', new)
            self.assertIn('name', new)
            self.assertEqual(1, new['id'])
            self.assertEqual('example', new['name'])
        finally:
            shutil.move(copy_fname, original_fname)

    def test_gen_pgt(self):

        c = RestClient('localhost', lgweb_port, 10)

        # doesn't exist!
        self.assertRaises(RestClientException, c._GET, '/gen_pgt?lg_name=doesnt_exist.json&num_par=5&algo=metis&min_goal=0&ptype=0&max_load_imb=100')
        # unknown algorithm
        self.assertRaises(RestClientException, c._GET, '/gen_pgt?lg_name=logical_graphs/chiles_simple.json&num_par=5&algo=noidea')
        # this should work now
        self._generate_pgt(c)

    def test_get_pgtjson(self):

        c = RestClient('localhost', lgweb_port, 10)
        c._GET('/gen_pgt?lg_name=logical_graphs/chiles_simple.json&num_par=5&algo=metis&min_goal=0&ptype=0&max_load_imb=100')

        # doesn't exist
        self.assertRaises(RestClientException, c._get_json, '/pgt_jsonbody?pgt_name=unknown.json')
        # good!
        c._get_json('/pgt_jsonbody?pgt_name=logical_graphs/chiles_simple1_pgt.json')

    def test_get_pgt_post(self):

        c = RestClient('localhost', lgweb_port, 10)

        # an API call with an empty form should cause an error
        empty_form_data = urllib.urlencode({})
        self.assertRaises(RestClientException, c._POST, '/gen_pgt')

        # new logical graph JSON
        json_data = "{\"modelData\":{\"fileType\":3,\"repo\":\"james-strauss-uwa/eagle-test\",\"filePath\":\"test\",\"sha\":\"1c6b3f778c5dc72a80001e7f2d08f639be9ad8a6\",\"git_url\":\"https://api.github.com/repos/james-strauss-uwa/eagle-test/git/blobs/1c6b3f778c5dc72a80001e7f2d08f639be9ad8a6\"},\"nodeDataArray\":[{\"category\":\"memory\",\"categoryType\":\"DataDrop\",\"type\":\"memory\",\"isData\":true,\"isGroup\":false,\"canHaveInputs\":true,\"canHaveOutputs\":true,\"colour\":\" #394BB2\",\"key\":-1,\"text\":\"Enter label\",\"loc\":\"200 100\",\"inputPorts\":[],\"outputPorts\":[{\"Id\":\"ab5ada14-04d7-4b03-816d-c43428d4e2e4\",\"IdText\":\"event\"}],\"inputLocalPorts\":[],\"outputLocalPorts\":[],\"appFields\":[],\"fields\":[{\"text\":\"Data volume\",\"name\":\"data_volume\",\"value\":\"5\"},{\"text\":\"Group end\",\"name\":\"group_end\",\"value\":\"0\"}]},{\"category\":\"BashShellApp\",\"categoryType\":\"ApplicationDrop\",\"type\":\"BashShellApp\",\"isData\":false,\"isGroup\":false,\"canHaveInputs\":true,\"canHaveOutputs\":true,\"colour\":\" #1C2833\",\"key\":-2,\"text\":\"Enter label\",\"loc\":\"400 200\",\"inputPorts\":[{\"Id\":\"c12aa833-43a9-4c1e-abaa-c77396010a31\",\"IdText\":\"event\"}],\"outputPorts\":[{\"Id\":\"47c421b8-5cdc-4ff7-ab7e-b75140f2d951\",\"IdText\":\"event\"}],\"inputLocalPorts\":[],\"outputLocalPorts\":[],\"appFields\":[],\"fields\":[{\"text\":\"Execution time\",\"name\":\"execution_time\",\"value\":\"5\"},{\"text\":\"Num CPUs\",\"name\":\"num_cpus\",\"value\":\"1\"},{\"text\":\"Group start\",\"name\":\"group_start\",\"value\":\"0\"},{\"text\":\"Arg01\",\"name\":\"Arg01\",\"value\":\"\"},{\"text\":\"Arg02\",\"name\":\"Arg02\",\"value\":\"\"},{\"text\":\"Arg03\",\"name\":\"Arg03\",\"value\":\"\"},{\"text\":\"Arg04\",\"name\":\"Arg04\",\"value\":\"\"},{\"text\":\"Arg05\",\"name\":\"Arg05\",\"value\":\"\"},{\"text\":\"Arg06\",\"name\":\"Arg06\",\"value\":\"\"},{\"text\":\"Arg07\",\"name\":\"Arg07\",\"value\":\"\"},{\"text\":\"Arg08\",\"name\":\"Arg08\",\"value\":\"\"},{\"text\":\"Arg09\",\"name\":\"Arg09\",\"value\":\"\"},{\"text\":\"Arg10\",\"name\":\"Arg10\",\"value\":\"\"}]},{\"category\":\"Component\",\"categoryType\":\"ApplicationDrop\",\"type\":\"Component\",\"isData\":false,\"isGroup\":false,\"canHaveInputs\":true,\"canHaveOutputs\":true,\"colour\":\" #3498DB\",\"key\":-3,\"text\":\"Enter label\",\"loc\":\"600 300\",\"inputPorts\":[{\"Id\":\"0178b7ce-79ed-406d-9e4b-ee2a53c168ec\",\"IdText\":\"event\"}],\"outputPorts\":[{\"Id\":\"f487361b-f633-43ba-978d-c65d7a52c34d\",\"IdText\":\"event\"}],\"inputLocalPorts\":[],\"outputLocalPorts\":[],\"appFields\":[],\"fields\":[{\"text\":\"Execution time\",\"name\":\"execution_time\",\"value\":\"5\"},{\"text\":\"Num CPUs\",\"name\":\"num_cpus\",\"value\":\"1\"},{\"text\":\"Group start\",\"name\":\"group_start\",\"value\":\"0\"},{\"text\":\"Appclass\",\"name\":\"appclass\",\"value\":\"test.graphsRepository\"},{\"text\":\"Arg01\",\"name\":\"Arg01\",\"value\":\"\"},{\"text\":\"Arg02\",\"name\":\"Arg02\",\"value\":\"\"},{\"text\":\"Arg03\",\"name\":\"Arg03\",\"value\":\"\"},{\"text\":\"Arg04\",\"name\":\"Arg04\",\"value\":\"\"},{\"text\":\"Arg05\",\"name\":\"Arg05\",\"value\":\"\"},{\"text\":\"Arg06\",\"name\":\"Arg06\",\"value\":\"\"},{\"text\":\"Arg07\",\"name\":\"Arg07\",\"value\":\"\"},{\"text\":\"Arg08\",\"name\":\"Arg08\",\"value\":\"\"},{\"text\":\"Arg09\",\"name\":\"Arg09\",\"value\":\"\"},{\"text\":\"Arg10\",\"name\":\"Arg10\",\"value\":\"\"}]},{\"category\":\"file\",\"categoryType\":\"DataDrop\",\"type\":\"file\",\"isData\":true,\"isGroup\":false,\"canHaveInputs\":true,\"canHaveOutputs\":true,\"colour\":\" #394BB2\",\"key\":-4,\"text\":\"Enter label\",\"loc\":\"800 400\",\"inputPorts\":[{\"Id\":\"c05689f4-6c5a-47dc-b3b1-fcbdfa09e4df\",\"IdText\":\"event\"}],\"outputPorts\":[],\"inputLocalPorts\":[],\"outputLocalPorts\":[],\"appFields\":[],\"fields\":[{\"text\":\"Data volume\",\"name\":\"data_volume\",\"value\":\"5\"},{\"text\":\"Group end\",\"name\":\"group_end\",\"value\":\"0\"},{\"text\":\"Check file path exists\",\"name\":\"check_filepath_exists\",\"value\":\"1\"},{\"text\":\"File path\",\"name\":\"filepath\",\"value\":\"\"},{\"text\":\"Directory name\",\"name\":\"dirname\",\"value\":\"\"}]}],\"linkDataArray\":[{\"from\":-1,\"fromPort\":\"ab5ada14-04d7-4b03-816d-c43428d4e2e4\",\"to\":-2,\"toPort\":\"c12aa833-43a9-4c1e-abaa-c77396010a31\"},{\"from\":-2,\"fromPort\":\"47c421b8-5cdc-4ff7-ab7e-b75140f2d951\",\"to\":-3,\"toPort\":\"0178b7ce-79ed-406d-9e4b-ee2a53c168ec\"},{\"from\":-3,\"fromPort\":\"f487361b-f633-43ba-978d-c65d7a52c34d\",\"to\":-4,\"toPort\":\"c05689f4-6c5a-47dc-b3b1-fcbdfa09e4df\"}]}"

        # add 'correct' data to the form
        form_data = {
            'algo': 'metis',
            'lg_name': 'metis.graph',
            'json_data': json_data,
            'num_islands': 0,
            'num_par': 1,
            'par_label': 'Partition',
            'max_load_imb': 100,
            'max_cpu': 8
            }

        # POST form to /gen_pgt
        try:
            content = urllib.urlencode(form_data)
            ret = c._POST('/gen_pgt', content, content_type='application/x-www-form-urlencoded')
        except RestClientException as e:
            self.fail(e)

    def test_load_lgeditor(self):

        c = RestClient('localhost', lgweb_port, 10)

        # doesn't exist
        self.assertRaises(RestClientException, c._get_json, '/lg_editor?lg_name=unknown.json')
        # Defaults to first LG
        c._GET('/lg_editor')
        # also fine, LG exists
        c._GET('/lg_editor?lg_name=logical_graphs/chiles_simple.json')

    def test_pg_viewer(self):

        c = RestClient('localhost', lgweb_port, 10)
        self._generate_pgt(c)

        # doesn't exist
        self.assertRaises(RestClientException, c._GET, '/pg_viewer?pgt_view_name=unknown.json')
        # Defaults to first PGT
        c._GET('/pg_viewer')
        # also fine, PGT exists
        c._GET('/pg_viewer?pgt_view_name=logical_graphs/chiles_simple1_pgt.json')


    def _test_pgt_action(self, path, unknown_fails):

        c = RestClient('localhost', lgweb_port, 10)
        self._generate_pgt(c)

        # doesn't exist
        if unknown_fails:
            self.assertRaises(RestClientException, c._GET, '/' + path + '?pgt_id=unknown.json')
        else:
            c._GET('/' + path + '?pgt_id=unknown.json')

        # exists
        c._GET('/' + path + '?pgt_id=logical_graphs/chiles_simple1_pgt.json')

    def test_show_gantt_chart(self):
        self._test_pgt_action('show_gantt_chart', False)

    def test_show_schedule_mat(self):
        self._test_pgt_action('show_schedule_mat', False)

    def test_get_gantt_chart(self):
        self._test_pgt_action('pgt_gantt_chart', True)
