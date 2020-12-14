import sys

import translator_client as tc


translator_config = tc.Configuration()
translator_config.host = "127.0.0.1:8084"

with open(sys.argv[1], 'rt') as f:
    graph = f.read()

with tc.ApiClient(translator_config) as translator_client:
    translator = tc.DefaultApi(translator_client)
    html_content = translator.gen_pgt(json_data=graph,
        lg_name='test', algo='metis', num_islands=1)
    print(html_content)
    html_content = translator.gen_pg(
        pgt_id='test', dlg_mgr_host='127.0.0.1', dlg_mgr_port=8001, dlg_mgr_deploy='deploy')
