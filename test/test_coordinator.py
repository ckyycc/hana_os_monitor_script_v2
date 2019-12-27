from unittest import TestCase
from unittest.mock import patch, MagicMock, call, ANY
from util import MonitorConst as Mc
from util import MonitorUtility as Mu
from coordinator import MonitorCoordinator
import paramiko


class TestMonitorCoordinator(TestCase):
    def setUp(self):
        self.cpu_interval = "CHECK_INTERVAL_CPU_INT"
        self.mem_interval = "CHECK_INTERVAL_MEM_INT"
        self.disk_interval = "CHECK_INTERVAL_DISK_INT"
        self.instance_interval = "CHECK_INTERVAL_INSTANCE_INT"

        self.server1 = "test_server1"
        self.server2 = "test_server2"
        self.server_list = [[1, self.server1, "/usr/sap", "SUSE"], [2, self.server2, "/usr/sap", "SUSE"]]

        self.config_server_list = [
                 {Mc.FIELD_SERVER_ID: 1, Mc.FIELD_SERVER_FULL_NAME: self.server1, Mc.FIELD_MOUNT_POINT: '/usr/sap',
                  Mc.FIELD_OS: 'SUSE'},
                 {Mc.FIELD_SERVER_ID: 2, Mc.FIELD_SERVER_FULL_NAME: self.server2, Mc.FIELD_MOUNT_POINT: '/usr/sap',
                  Mc.FIELD_OS: 'SUSE'}
             ]
        self.config_msg = {
            self.cpu_interval: 1, self.mem_interval: 2, self.disk_interval: 3, self.instance_interval: 4,
            Mc.DB_CONFIGURATION_SERVER: self.config_server_list}

        Mu.open_ssh_connection = MagicMock(return_value=paramiko.SSHClient())

    def test_initialize_of_coordinator(self):
        coordinator, mock_os_operator, mock_consumer = self.__get_mock_objects()
        # mock the heartbeat function
        coordinator._MonitorCoordinator__process_heartbeat = MagicMock(return_value=None)
        # start coordinator
        coordinator._MonitorCoordinator__coordinating_monitors(self.__get_mock_msg_list([self.config_msg]))
        # should tried to connect the two servers
        Mu.open_ssh_connection.assert_has_calls([
            call(ANY, ANY, self.server1, Mc.get_ssh_default_user(), Mc.get_ssh_default_password()),
            call(ANY, ANY, self.server2, Mc.get_ssh_default_user(), Mc.get_ssh_default_password())
        ])

        # should tried to start agent on two servers
        mock_os_operator.restart_agent.assert_has_calls([
            call(ANY, 1, "/usr/sap", Mc.get_agent_path(), self.config_msg[self.mem_interval],
                 self.config_msg[self.cpu_interval], self.config_msg[self.disk_interval],
                 self.config_msg[self.instance_interval]),
            call(ANY, 2, "/usr/sap", Mc.get_agent_path(), self.config_msg[self.mem_interval],
                 self.config_msg[self.cpu_interval], self.config_msg[self.disk_interval],
                 self.config_msg[self.instance_interval])
        ])

    def test_start_heartbeat_after_received_all_configuration(self):
        coordinator, mock_os_operator, mock_consumer = self.__get_mock_objects()
        # mock the heartbeat function
        coordinator._MonitorCoordinator__process_heartbeat = MagicMock(return_value=None)
        # start coordinator
        coordinator._MonitorCoordinator__coordinating_monitors(self.__get_mock_msg_list([self.config_msg]))
        coordinator._MonitorCoordinator__process_heartbeat.assert_called_once()

    def test_should_not_start_heartbeat_if_not_received_all_configuration(self):
        coordinator, mock_os_operator, mock_consumer = self.__get_mock_objects()
        # mock the heartbeat function
        coordinator._MonitorCoordinator__process_heartbeat = MagicMock(return_value=None)
        # start coordinator
        config_msg = self.config_msg.copy()
        config_msg.pop(self.cpu_interval)  # pop one config item out
        coordinator._MonitorCoordinator__coordinating_monitors(self.__get_mock_msg_list([config_msg]))
        coordinator._MonitorCoordinator__process_heartbeat.assert_not_called()

    def test_coordinating_monitors_should_restart_all_agents_if_configuration_changed(self):
        # initialize
        coordinator, mock_os_operator, mock_consumer = self.__get_mock_objects()
        # mock the heartbeat function
        coordinator._MonitorCoordinator__process_heartbeat = MagicMock(return_value=None)
        # start coordinator
        coordinator._MonitorCoordinator__coordinating_monitors(self.__get_mock_msg_list([self.config_msg]))

        # change the configurations
        config_msg = self.config_msg.copy()
        config_msg[self.cpu_interval] = 25
        coordinator._MonitorCoordinator__coordinating_monitors(self.__get_mock_msg_list([config_msg]))

        # should tried to connect the two servers
        Mu.open_ssh_connection.assert_has_calls([
            call(ANY, ANY, self.server1, Mc.get_ssh_default_user(), Mc.get_ssh_default_password()),
            call(ANY, ANY, self.server2, Mc.get_ssh_default_user(), Mc.get_ssh_default_password()),
            call(ANY, ANY, self.server1, Mc.get_ssh_default_user(), Mc.get_ssh_default_password()),
            call(ANY, ANY, self.server2, Mc.get_ssh_default_user(), Mc.get_ssh_default_password())
        ], any_order=False)

        # should tried to start agent on two servers
        mock_os_operator.restart_agent.assert_has_calls([
            call(ANY, 1, "/usr/sap", Mc.get_agent_path(), self.config_msg[self.mem_interval],
                 self.config_msg[self.cpu_interval], self.config_msg[self.disk_interval],
                 self.config_msg[self.instance_interval]),
            call(ANY, 2, "/usr/sap", Mc.get_agent_path(), self.config_msg[self.mem_interval],
                 self.config_msg[self.cpu_interval], self.config_msg[self.disk_interval],
                 self.config_msg[self.instance_interval]),
            call(ANY, 1, "/usr/sap", Mc.get_agent_path(), config_msg[self.mem_interval],
                 config_msg[self.cpu_interval], config_msg[self.disk_interval],
                 config_msg[self.instance_interval]),
            call(ANY, 2, "/usr/sap", Mc.get_agent_path(), config_msg[self.mem_interval],
                 config_msg[self.cpu_interval], config_msg[self.disk_interval],
                 config_msg[self.instance_interval])
        ], any_order=False)

    def test_coordinating_monitors_should_not_restart_any_agent_if_configuration_not_changed(self):
        # initialize
        coordinator, mock_os_operator, mock_consumer = self.__get_mock_objects()
        # mock the heartbeat function
        coordinator._MonitorCoordinator__process_heartbeat = MagicMock(return_value=None)
        # start coordinator
        coordinator._MonitorCoordinator__coordinating_monitors(self.__get_mock_msg_list([self.config_msg]))

        # change the configurations
        config_msg = self.config_msg.copy()
        coordinator._MonitorCoordinator__coordinating_monitors(self.__get_mock_msg_list([config_msg]))

        # should tried to start agent on two servers
        self.assertEqual(mock_os_operator.restart_agent.call_count, 2)
        self.assertEqual(Mu.open_ssh_connection.call_count, 2)

    @patch("util.KafkaConsumer")
    @patch("coordinator.LinuxOperator")
    def __get_mock_objects(self, mock_os_operator, mock_consumer):
        coordinator = MonitorCoordinator()
        return coordinator, mock_os_operator.return_value, mock_consumer.return_value

    @staticmethod
    def __get_mock_msg_list(msg_list):
        class Infos:
            def __init__(self, info_list):
                self.infos = []
                for msg in info_list:
                    info = lambda: None
                    setattr(info, "value", {})
                    info.value = msg
                    self.infos.append(info)

            def __iter__(self):
                return iter(self.infos)

        return Infos(msg_list)
