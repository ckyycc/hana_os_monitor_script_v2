from util import MonitorUtility as Mu
from util import MonitorConst as Mc
from operation.os_operations import LinuxOperator
from operation.db_operations import HANAMonitorDAO
import os


class AgentPublisher:
    def __init__(self):
        self.__os_operator = LinuxOperator()
        operator = HANAMonitorDAO(Mc.get_hana_server(), Mc.get_hana_port(), Mc.get_hana_user(), Mc.get_hana_password())
        self.servers = self.__get_servers(operator)
        # self.servers = []
        # move belows to config?
        self.path = Mc.get_agent_path()[:-len('agent.py')]
        self.files = ['agent.py', 'util.py', 'errors.py', 'config/configuration.ini', 'config/logging.ini']

    def publish(self):
        user = Mc.get_ssh_default_user()
        password = Mc.get_ssh_default_password()
        host_name = os.uname()[1]
        for server in self.servers:
            if host_name in server:
                Mu.log_debug(None, "Skipping local server on {0}".format(server))
                continue
            with Mu.open_ssh_connection(None,
                                        self.__os_operator,
                                        server,
                                        user,
                                        password) as ssh:
                Mu.log_debug(None, "Publishing agent on {0}".format(server))
                for file in self.files:
                    # Currently, path for source and target is the same
                    source = self.path + file
                    target = self.path + file
                    self.__os_operator.upload_file(ssh, source, target)
                Mu.log_debug(None, "Publishing agent on {0} is done".format(server))

    def __get_servers(self, operator):
        # get all servers info from db
        db_output_servers = operator.get_server_full_names()
        try:
            servers = [server[1] for server in db_output_servers]
        except Exception as ex:
            Mu.log_warning(None,
                           "Parsing DB output failed in 'get_server_full_names' with error: {0}, the output: {1}"
                           .format(ex, db_output_servers))
            servers = []

        return servers


if __name__ == '__main__':
    AgentPublisher().publish()
