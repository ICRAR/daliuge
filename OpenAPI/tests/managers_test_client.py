import composite_manager_client as cmc
import node_manager_client as nmc
from composite_manager_client.api.default_api import DefaultApi

nm_config = nmc.Configuration()
nm_config.host = "localhost:8000"
dim_config = cmc.Configuration()
dim_config.host = "localhost:8001"

with nmc.ApiClient(nm_config) as nm_client, cmc.ApiClient(dim_config) as dim_client:
    dim = DefaultApi(dim_client)
    nm = DefaultApi(nm_client)

    print("sessions: %r" % (dim.get_sessions(),))
    dim.create_session(inline_object={"sessionId": "abc"})
    print("sessions in NM: %r" % (nm.get_sessions(),))
    print("sessions in DIM: %r" % (dim.get_sessions(),))
    nodes = dim.get_cm_nodes()
    print(nodes)
    if "localhost" not in nodes:
        raise ValueError()
