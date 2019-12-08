from unittest import TestCase
from unittest.mock import patch
from util import MonitorConst as Mc
from config_monitor import ConfigMonitor


class TestConfigMonitor(TestCase):
    def setUp(self):
        self.cpu_threshold = "THRESHOLD_CPU_USAGE_WARN_INT"
        self.mem_threshold = "THRESHOLD_MEM_USAGE_WARN_INT"
        self.disk_threshold = "THRESHOLD_DISK_USAGE_WARN_INT"
        self.email_sender = "EMAIL_SENDER"
        self.config_list = [[self.cpu_threshold, 1], [self.mem_threshold, 2], [self.disk_threshold, 3],
                            [self.email_sender, "test@test"]]
        self.server_list = [[1, "test_server1", "/usr/sap", "SUSE"], [2, "test_server2", "/usr/sap", "SUSE"]]

        self.config_server_list = [
            {Mc.FIELD_SERVER_ID: 1, Mc.FIELD_SERVER_FULL_NAME: 'test_server1', Mc.FIELD_MOUNT_POINT: '/usr/sap',
             Mc.FIELD_OS: 'SUSE'},
            {Mc.FIELD_SERVER_ID: 2, Mc.FIELD_SERVER_FULL_NAME: 'test_server2', Mc.FIELD_MOUNT_POINT: '/usr/sap',
                  Mc.FIELD_OS: 'SUSE'}
             ]
        self.config_msg = {self.cpu_threshold: 1, self.mem_threshold: 2, self.disk_threshold: 3,
                           self.email_sender: "test@test", Mc.DB_CONFIGURATION_SERVER: self.config_server_list}

    def test_initialize(self):
        config_monitor, mock_dao, mock_producer = self.__get_mock_objects()
        mock_dao.get_configuration.return_value = self.config_list
        mock_dao.get_server_full_names.return_value = self.server_list
        config_monitor._ConfigMonitor__monitoring_configurations(mock_dao)

        mock_dao.get_configuration.assert_called_once()
        mock_dao.get_server_full_names.assert_called_once()
        mock_producer.send.assert_called_once_with(Mc.TOPIC_CONFIGURATION, self.config_msg)

    def test_config_change(self):
        config_monitor, mock_dao, mock_producer = self.__get_mock_objects()
        # initialize
        mock_dao.get_configuration.return_value = self.config_list
        mock_dao.get_server_full_names.return_value = self.server_list
        config_monitor._ConfigMonitor__monitoring_configurations(mock_dao)

        # change
        config_list = self.config_list.copy()
        config_list[0][1] = 5  # change cpu_threshold from 1 to 5
        config_list[3][1] = "ck@test"  # change email_sender to a new value "ck@test"
        operation_time = "OPERATION_TIME"
        operation_time_value = "8,18"
        config_list.append([operation_time, "8,18"])  # add a new configuration item: OPERATION_TIME
        mock_dao.get_configuration.return_value = config_list
        server_list = self.server_list.copy()
        server_list[0][2] = "/data"  # change mount point
        mock_dao.get_server_full_names.return_value = server_list

        config_monitor._ConfigMonitor__monitoring_configurations(mock_dao)

        config_server_list = self.config_server_list.copy()
        config_server_list[0][Mc.FIELD_MOUNT_POINT] = server_list[0][2]

        config_msg = {
            self.cpu_threshold: config_list[0][1], self.email_sender: config_list[3][1],
            operation_time: operation_time_value,
            Mc.DB_CONFIGURATION_SERVER: config_server_list}

        mock_producer.send.assert_called_with(Mc.TOPIC_CONFIGURATION, config_msg)

    @patch("config_monitor.HANAMonitorDAO")
    @patch("config_monitor.KafkaProducer")
    def __get_mock_objects(self, mock_producer, mock_dao):
        config_monitor = ConfigMonitor()
        return config_monitor, mock_dao.return_value, mock_producer.return_value

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
