from unittest import TestCase
from unittest.mock import MagicMock, call, patch, ANY

from util import ActionType
from util import MonitorUtility as Mu
from util import MonitorConst as Mc

from app_operator import AppOperator
from kafka import KafkaConsumer
import paramiko


class TestAppOperator(TestCase):
    def setUp(self):
        self.server_name = "Test_Server"
        self.sid = "CK1"
        self.user_name = "ck1adm"
        self.shutdown_msg = {ActionType.SHUTDOWN.value: {
            Mc.FIELD_SERVER_FULL_NAME: self.server_name, Mc.FIELD_SID: self.sid, Mc.FIELD_USER_NAME: self.user_name}}
        self.cleaning_msg = {ActionType.CLEAN_LOG_BACKUP.value: {
            Mc.FIELD_SERVER_FULL_NAME: self.server_name, Mc.FIELD_SID: self.sid, Mc.FIELD_USER_NAME: self.user_name}}
        # mock ssh connection, return an empty ssh client
        Mu.open_ssh_connection = MagicMock(return_value=paramiko.SSHClient())

    def test_shutdown(self):
        operator, mock_os_operator, mock_consumer = self.__get_mock_operator()
        mock_consumer.poll.return_value = {"test_topic": self.__get_mock_msg_list([self.shutdown_msg])}

        operator._AppOperator__operate(mock_consumer)
        Mu.open_ssh_connection.assert_called_once_with(
            ANY, ANY, self.server_name, self.user_name, Mc.get_ssh_default_password())
        mock_os_operator.return_value.shutdown_hana.assert_called_once()

    def test_cleaning(self):
        operator, mock_os_operator, mock_consumer = self.__get_mock_operator()
        mock_consumer.poll.return_value = {"test_topic": self.__get_mock_msg_list([self.cleaning_msg])}

        operator._AppOperator__operate(mock_consumer)
        Mu.open_ssh_connection.assert_called_once_with(
            ANY, ANY, self.server_name, self.user_name, Mc.get_ssh_default_password())
        mock_os_operator.return_value.clean_log_backup.assert_called_once()

    def test_shutdown_and_cleaning(self):
        operator, mock_os_operator, mock_consumer = self.__get_mock_operator()
        mock_consumer.poll.return_value = {"t_topic": self.__get_mock_msg_list([self.shutdown_msg, self.cleaning_msg])}

        operator._AppOperator__operate(mock_consumer)
        ssh_call = call(ANY, ANY, self.server_name, self.user_name, Mc.get_ssh_default_password())
        Mu.open_ssh_connection.assert_has_calls([ssh_call, ssh_call])
        mock_os_operator.return_value.shutdown_hana.assert_called_once()
        mock_os_operator.return_value.clean_log_backup.assert_called_once()

    @patch("test_app_operator.KafkaConsumer")
    @patch("app_operator.LinuxOperator")
    def __get_mock_operator(self, mock_os_operator, mock_consumer):
        app_operator = AppOperator()
        consumer = KafkaConsumer(group_id="TEST_GROUP", bootstrap_servers=["test_server", 9092])
        return app_operator, mock_os_operator, mock_consumer

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
