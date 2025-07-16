import logging
from dlg.manager import client

logger = logging.getLogger(f"dlg.{__name__}")


class NMAssigner:
    """
    Handles the logic of assigning / removing NMs from DIMs && handling new DIMs coming on/off-line.
    Maintains a leger of DIMs and NMs as name -> (server, port) mappings.
    Maintains a mapping of NMs to DIMs
    Allocation logic is currently very simple, handling only the case of a single DIM, but with
    room for more advanced load-balancing, if we require in the future.
    """

    def __init__(self):
        self.DIMs = {}  # Set of DIMs   name -> (server, port)
        self.NMs = {}  # Set of NMs     name -> (server, port)
        self.assignments = {}  # Maps NMs to DIMs
        self.mm_client = client.MasterManagerClient()

    def add_dim(self, name, server, port):
        self.DIMs[name] = (server, port)
        self.mm_client.add_dim(server)
        self.allocate_nms()

    def add_nm(self, name, server, port):
        self.NMs[name] = (server, port)
        self.mm_client.add_node(server)
        self.allocate_nms()

    def get_dim(self, name):
        return self.DIMs[name]

    def get_nm(self, name):
        return self.NMs[name]

    def remove_dim(self, name):
        server, port = self.DIMs[name]
        try:
            for nm in self.NMs:
                if self.assignments[nm] == server:
                    del self.assignments[nm]
            self.mm_client.remove_dim(server)
        finally:
            del self.DIMs[name]
        return server, port

    def remove_nm(self, name):
        server, _ = self.NMs[name]
        try:
            if name in self.assignments:
                dim_ip = self.assignments[name]
                nm_ip = self.NMs[name][0]
                self.mm_client.remove_node_from_dim(dim_ip, nm_ip)
            self.mm_client.remove_node(server)
        finally:
            del self.NMs[name]

    def allocate_nms(self):
        if self.DIMs == {}:
            logger.info("We have no DIMs")
        elif len(self.DIMs.keys()) == 1:
            dim_ip = list(self.DIMs.values())[0][0]  # IP of the first (only) DIM
            for nm, endpoint in self.NMs.items():
                nm_ip = endpoint[0]
                if nm not in self.assignments:
                    logger.info("Adding NM %s to DIM %s", nm_ip, dim_ip)
                    self.mm_client.add_node_to_dim(dim_ip, nm_ip)
                    self.assignments[nm] = dim_ip
                elif self.assignments[nm] not in self.DIMs:  # If we've removed a DIM
                    logger.info("Re-assigning %s to DIM %s", nm_ip, dim_ip)
                    self.mm_client.add_node_to_dim(dim_ip, nm_ip)
                    self.assignments[nm] = dim_ip
        else:  # We have lots of DIMs
            # Will do nothing, it's up to the user/deployer to handle this.
            logger.info("Multiple DIMs, handle node assignments externally.")
