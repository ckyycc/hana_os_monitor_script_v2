from unittest import TestCase
from unittest.mock import MagicMock, call, patch

from util import MonitorUtility as Mu
from util import MonitorConst as Mc
from util import KafKaUtility as Ku
from util import Email, InfoType

from alarm_operator import AlarmOperator


class TestAlarmOperator(TestCase):
    def setUp(self):
        self.server_id = 1
        self.check_id = "20191125010101001"
        self.user_info = [
            {Mc.FIELD_SERVER_FULL_NAME: "test_server1",
             Mc.FIELD_SID: "CK1",
             Mc.FIELD_USER_NAME: "ck1adm",
             Mc.FIELD_FILTER_FLAG: "",
             Mc.FIELD_EMPLOYEE_NAME: "employee_1",
             Mc.FIELD_EMAIL: "employ1@test.com"},
            {Mc.FIELD_SERVER_FULL_NAME: "test_server1",
             Mc.FIELD_SID: "CK2",
             Mc.FIELD_USER_NAME: "ck2adm",
             Mc.FIELD_FILTER_FLAG: "",
             Mc.FIELD_EMPLOYEE_NAME: "employee_2",
             Mc.FIELD_EMAIL: "employ2@test.com"},
            {Mc.FIELD_SERVER_FULL_NAME: "test_server1",
             Mc.FIELD_SID: "CK3",
             Mc.FIELD_USER_NAME: "ck3adm",
             Mc.FIELD_FILTER_FLAG: "",
             Mc.FIELD_EMPLOYEE_NAME: "employee_3",
             Mc.FIELD_EMAIL: "employ3@test.com"}
        ]

        self.admin_email = "test@test.com"

        self.mem_total, self.mem_free = 1000000000, 2500000
        self.mem_msg_list = [{
            Mc.MSG_TYPE: InfoType.MEMORY.value, Mc.MSG_INFO: {"ck1adm": 15, 'ck2adm': 25, 'ck3adm': 35},
            Mc.FIELD_CHECK_ID: self.check_id, Mc.FIELD_SERVER_ID: self.server_id, Mc.FIELD_MEM_TOTAL: self.mem_total,
            Mc.FIELD_MEM_FREE: self.mem_free}]

        self.disk_total, self.disk_free = 1234567890, 34567890
        self.disk_msg_list = [{
            Mc.MSG_TYPE: InfoType.DISK.value, Mc.MSG_INFO:
                {"folder1": {"ck1adm": 10000}, 'folder2': {'ck2adm': 20000}, 'folder3': {'ck3adm': 30000}},
            Mc.FIELD_CHECK_ID: self.check_id, Mc.FIELD_SERVER_ID: self.server_id, Mc.FIELD_DISK_TOTAL: self.disk_total,
            Mc.FIELD_DISK_FREE: self.disk_free}]

        self.cpu_number, self.cpu_usage = 512, 88
        self.cpu_msg_list = [{
            Mc.MSG_TYPE: InfoType.CPU.value, Mc.MSG_INFO: {"ck1adm": 18, 'ck2adm': 28, 'ck3adm': 38},
            Mc.FIELD_CHECK_ID: self.check_id, Mc.FIELD_SERVER_ID: self.server_id, Mc.FIELD_CPU_NUMBER: self.cpu_number,
            Mc.FIELD_CPU_UTILIZATION: self.cpu_usage}]

        Mu.generate_check_id = MagicMock(return_value=self.check_id)
        Ku.assign_and_seek_to_end = MagicMock(return_value=None)
        Mu.is_current_time_working_time = MagicMock(return_value=True)
        Email.send_emergency_shutdown_email = MagicMock(return_value=None)
        Email.send_warning_email = MagicMock(return_value=None)
        Email.send_shutdown_email = MagicMock(return_value=None)
        Email.send_cleaning_disk_email = MagicMock(return_value=None)

    def test_configuration_start_heartbeat_after_configuration_is_done(self):
        infos = TestAlarmOperator.__get_mock_msg_list(TestAlarmOperator.__get_config_info())
        operator = self.__get_mock_operator()
        operator._AlarmOperator__process_heartbeat = MagicMock(return_value=None)
        operator._AlarmOperator__operate(infos)
        operator._AlarmOperator__process_heartbeat.assert_called_once()

    def test_emergency_alert(self):
        operator = self.__get_mock_operator()
        operator.mem_emergency_threshold = 5
        operator.mem_threshold = 95
        infos = TestAlarmOperator.__get_mock_msg_list(self.mem_msg_list)

        operator._AlarmOperator__operate(infos)

        Email.send_emergency_shutdown_email.assert_called_once_with(
            "", "employ3@test.com", "CK3", "test_server1", "employee_3", self.admin_email, 35, InfoType.MEMORY)
        operator._AlarmOperator__send_shutdown_message.assert_called_once_with("test_server1", "CK3", "ck3adm")

    def test_emergency_alert_should_skip_filtered_sid(self):
        operator = self.__get_mock_operator()
        operator.mem_emergency_threshold = 90
        operator.mem_threshold = 95
        infos = TestAlarmOperator.__get_mock_msg_list(self.mem_msg_list)
        self.user_info[2][Mc.FIELD_FILTER_FLAG] = 'X'
        operator._AlarmOperator__operate(infos)

        Email.send_emergency_shutdown_email.assert_called_once_with(
            "", "employ2@test.com", "CK2", "test_server1", "employee_2", self.admin_email, 25, InfoType.MEMORY)
        operator._AlarmOperator__send_shutdown_message.assert_called_once_with("test_server1", "CK2", "ck2adm")

    def test_memory_high_usage_alert(self):
        operator = self.__get_mock_operator()
        operator.mem_emergency_threshold = 100  # disable emergency
        operator.mem_threshold = 80
        infos = TestAlarmOperator.__get_mock_msg_list(self.mem_msg_list)

        operator._AlarmOperator__operate(infos)
        Email.send_warning_email.assert_called_once_with(
            "", ['employ3@test.com', 'employ2@test.com', 'employ1@test.com'], InfoType.MEMORY,
            'test_server1', self.__get_mem_email_call_usage_info(), 'test@test.com')

    def test_memory_high_usage_shutdown_after_3_fails(self):
        operator = self.__get_mock_operator()
        operator.mem_emergency_threshold = 100  # disable emergency
        operator.mem_threshold = 80
        operator.check_interval = 0
        mem_list = self.mem_msg_list + self.mem_msg_list + self.mem_msg_list + self.mem_msg_list
        infos = TestAlarmOperator.__get_mock_msg_list(mem_list)

        operator._AlarmOperator__operate(infos)
        warning_email_call = call(
            "", ['employ3@test.com', 'employ2@test.com', 'employ1@test.com'], InfoType.MEMORY,
            'test_server1', self.__get_mem_email_call_usage_info(), 'test@test.com')
        calls = [warning_email_call, warning_email_call, warning_email_call, warning_email_call]
        # should send 4 times warning email
        Email.send_warning_email.assert_has_calls(calls)
        # should send 1 time shutdown email
        Email.send_shutdown_email.assert_called_once_with(
            "", "employ3@test.com", "CK3", "test_server1", "employee_3", self.admin_email, 35, InfoType.MEMORY)
        operator._AlarmOperator__send_shutdown_message.assert_called_once_with("test_server1", "CK3", "ck3adm")

    def test_memory_high_usage_shutdown_after_3_fails_should_skip_filtered_sid(self):
        self.user_info[2][Mc.FIELD_FILTER_FLAG] = 'X'
        operator = self.__get_mock_operator()
        operator.mem_emergency_threshold = 100  # disable emergency
        operator.mem_threshold = 80
        operator.check_interval = 0

        mem_list = self.mem_msg_list + self.mem_msg_list + self.mem_msg_list + self.mem_msg_list
        infos = TestAlarmOperator.__get_mock_msg_list(mem_list)

        operator._AlarmOperator__operate(infos)
        warning_email_call = call(
            "", ['employ3@test.com', 'employ2@test.com', 'employ1@test.com'], InfoType.MEMORY,
            'test_server1', self.__get_mem_email_call_usage_info(True), 'test@test.com')
        calls = [warning_email_call, warning_email_call, warning_email_call, warning_email_call]
        # should send 4 times warning email
        Email.send_warning_email.assert_has_calls(calls)
        # should send 1 time shutdown email
        Email.send_shutdown_email.assert_called_once_with(
            "", "employ2@test.com", "CK2", "test_server1", "employee_2", self.admin_email, 25, InfoType.MEMORY)
        operator._AlarmOperator__send_shutdown_message.assert_called_once_with("test_server1", "CK2", "ck2adm")

    def test_disk_high_usage_alert(self):
        operator = self.__get_mock_operator()
        operator.disk_threshold = 80
        infos = TestAlarmOperator.__get_mock_msg_list(self.disk_msg_list)

        operator._AlarmOperator__operate(infos)
        Email.send_warning_email.assert_called_once_with(
            "", ['employ3@test.com', 'employ2@test.com', 'employ1@test.com'], InfoType.DISK,
            'test_server1', self.__get_disk_email_call_usage_info(), 'test@test.com')

    def test_disk_high_usage_alert_cleaning_after_3_fails(self):
        operator = self.__get_mock_operator()
        operator.disk_threshold = 80
        info_list = self.disk_msg_list + self.disk_msg_list + self.disk_msg_list + self.disk_msg_list
        infos = TestAlarmOperator.__get_mock_msg_list(info_list)
        operator._AlarmOperator__operate(infos)

        warning_email_call = call(
            "", ['employ3@test.com', 'employ2@test.com', 'employ1@test.com'], InfoType.DISK,
            'test_server1', self.__get_disk_email_call_usage_info(), 'test@test.com')

        calls = [warning_email_call, warning_email_call, warning_email_call, warning_email_call]
        Email.send_warning_email.assert_has_calls(calls)
        operator._AlarmOperator__send_cleaning_message.assert_called_once_with("test_server1", "folder3", "ck3adm")

    def test_disk_high_usage_alert_cleaning_after_3_fails_should_skip_filtered_sid(self):
        self.user_info[2][Mc.FIELD_FILTER_FLAG] = 'X'
        operator = self.__get_mock_operator()
        operator.disk_threshold = 80
        info_list = self.disk_msg_list + self.disk_msg_list + self.disk_msg_list + self.disk_msg_list
        infos = TestAlarmOperator.__get_mock_msg_list(info_list)
        operator._AlarmOperator__operate(infos)

        warning_email_call = call(
            "", ['employ3@test.com', 'employ2@test.com', 'employ1@test.com'], InfoType.DISK,
            'test_server1', self.__get_disk_email_call_usage_info(True), 'test@test.com')

        calls = [warning_email_call, warning_email_call, warning_email_call, warning_email_call]
        Email.send_warning_email.assert_has_calls(calls)
        operator._AlarmOperator__send_cleaning_message.assert_called_once_with("test_server1", "folder2", "ck2adm")

    def test_cpu_high_usage_alert(self):
        operator = self.__get_mock_operator()
        operator.cpu_threshold = 80
        infos = TestAlarmOperator.__get_mock_msg_list(self.cpu_msg_list)

        operator._AlarmOperator__operate(infos)
        Email.send_warning_email.assert_called_once_with(
            "",
            ['employ3@test.com', 'employ2@test.com', 'employ1@test.com'],
            InfoType.CPU,
            'test_server1',
            {Mc.MSG_TYPE: InfoType.CPU, Mc.FIELD_SERVER_ID: 1, Mc.FIELD_SERVER_FULL_NAME: 'test_server1',
             Mc.FIELD_CHECK_ID: '20191125010101001', Mc.INFO_TOTAL: -1, Mc.INFO_FREE: 100 - self.cpu_usage,
             Mc.INFO_USAGE: [
                 {Mc.FIELD_SERVER_FULL_NAME: 'test_server1', Mc.FIELD_SID: 'CK3', Mc.FIELD_USER_NAME: 'ck3adm',
                  Mc.FIELD_FILTER_FLAG: '', Mc.FIELD_EMPLOYEE_NAME: 'employee_3', Mc.FIELD_EMAIL: 'employ3@test.com',
                  Mc.FIELD_USAGE: 38 / 512},
                 {Mc.FIELD_SERVER_FULL_NAME: 'test_server1', Mc.FIELD_SID: 'CK2', Mc.FIELD_USER_NAME: 'ck2adm',
                  Mc.FIELD_FILTER_FLAG: '', Mc.FIELD_EMPLOYEE_NAME: 'employee_2', Mc.FIELD_EMAIL: 'employ2@test.com',
                  Mc.FIELD_USAGE: 28 / 512},
                 {Mc.FIELD_SERVER_FULL_NAME: 'test_server1', Mc.FIELD_SID: 'CK1', Mc.FIELD_USER_NAME: 'ck1adm',
                  Mc.FIELD_FILTER_FLAG: '', Mc.FIELD_EMPLOYEE_NAME: 'employee_1', Mc.FIELD_EMAIL: 'employ1@test.com',
                  Mc.FIELD_USAGE: 18 / 512}]},
            'test@test.com')

    @patch("util.KafkaProducer")
    @patch("alarm_operator.HANAMonitorDAO")
    def __get_mock_operator(self, mock_hana_dao, mock_producer):
        operator = AlarmOperator()
        operator._AlarmOperator__db_operator.get_email_admin = MagicMock(return_value=self.admin_email)
        operator._AlarmOperator__db_operator.get_users_info = MagicMock(return_value=self.user_info)
        operator._AlarmOperator__send_shutdown_message = MagicMock(return_value=None)
        operator._AlarmOperator__send_cleaning_message = MagicMock(return_value=None)

        return operator

    def __get_mem_email_call_usage_info(self, filter_flag=False):
        usage_info = {
            Mc.MSG_TYPE: InfoType.MEMORY, Mc.FIELD_SERVER_ID: 1, Mc.FIELD_SERVER_FULL_NAME: 'test_server1',
            Mc.FIELD_CHECK_ID: '20191125010101001', Mc.INFO_TOTAL: self.mem_total, Mc.INFO_FREE: self.mem_free,
            Mc.INFO_USAGE: [
                {Mc.FIELD_SERVER_FULL_NAME: 'test_server1', Mc.FIELD_SID: 'CK3', Mc.FIELD_USER_NAME: 'ck3adm',
                 Mc.FIELD_FILTER_FLAG: '', Mc.FIELD_EMPLOYEE_NAME: 'employee_3', Mc.FIELD_EMAIL: 'employ3@test.com',
                 Mc.FIELD_USAGE: 35},
                {Mc.FIELD_SERVER_FULL_NAME: 'test_server1', Mc.FIELD_SID: 'CK2', Mc.FIELD_USER_NAME: 'ck2adm',
                 Mc.FIELD_FILTER_FLAG: '', Mc.FIELD_EMPLOYEE_NAME: 'employee_2', Mc.FIELD_EMAIL: 'employ2@test.com',
                 Mc.FIELD_USAGE: 25},
                {Mc.FIELD_SERVER_FULL_NAME: 'test_server1', Mc.FIELD_SID: 'CK1', Mc.FIELD_USER_NAME: 'ck1adm',
                 Mc.FIELD_FILTER_FLAG: '', Mc.FIELD_EMPLOYEE_NAME: 'employee_1',
                 Mc.FIELD_EMAIL: 'employ1@test.com',
                 Mc.FIELD_USAGE: 15}]
        }
        if filter_flag:
            usage_info[Mc.INFO_USAGE][0][Mc.FIELD_FILTER_FLAG] = 'X'

        return usage_info

    def __get_disk_email_call_usage_info(self, filter_flag=False):
        usage_info = {
            Mc.MSG_TYPE: InfoType.DISK, Mc.FIELD_SERVER_ID: 1, Mc.FIELD_SERVER_FULL_NAME: 'test_server1',
            Mc.FIELD_CHECK_ID: '20191125010101001', Mc.INFO_TOTAL: self.disk_total, Mc.INFO_FREE: self.disk_free,
            Mc.INFO_USAGE: [
                {Mc.FIELD_FOLDER: 'folder3', Mc.FIELD_USER_NAME: 'ck3adm', Mc.FIELD_USAGE: 30000,
                 Mc.FIELD_SERVER_FULL_NAME: 'test_server1', Mc.FIELD_SID: 'CK3', 'FILTER_FLAG': '',
                 Mc.FIELD_EMPLOYEE_NAME: 'employee_3', Mc.FIELD_EMAIL: 'employ3@test.com'},
                {Mc.FIELD_FOLDER: 'folder2', Mc.FIELD_USER_NAME: 'ck2adm', Mc.FIELD_USAGE: 20000,
                 Mc.FIELD_SERVER_FULL_NAME: 'test_server1', Mc.FIELD_SID: 'CK2', 'FILTER_FLAG': '',
                 Mc.FIELD_EMPLOYEE_NAME: 'employee_2', Mc.FIELD_EMAIL: 'employ2@test.com'},
                {Mc.FIELD_FOLDER: 'folder1', Mc.FIELD_USER_NAME: 'ck1adm', Mc.FIELD_USAGE: 10000,
                 Mc.FIELD_SERVER_FULL_NAME: 'test_server1', Mc.FIELD_SID: 'CK1', 'FILTER_FLAG': '',
                 Mc.FIELD_EMPLOYEE_NAME: 'employee_1', Mc.FIELD_EMAIL: 'employ1@test.com'}]}
        if filter_flag:
            usage_info[Mc.INFO_USAGE][0][Mc.FIELD_FILTER_FLAG] = 'X'

        return usage_info

    @staticmethod
    def __get_mock_msg_list(msg_list, assignment=0):
        class Infos:
            def __init__(self, msg_list, assignment_count):
                self.infos = []
                self.assignments = [1, 2] if assignment_count > 1 else []
                for msg in msg_list:
                    info = lambda: None
                    setattr(info, "value", {})
                    info.value = msg
                    self.infos.append(info)

            def __iter__(self):
                return iter(self.infos)

            def assignment(self):
                return self.assignments

        return Infos(msg_list, assignment)

    @staticmethod
    def __get_config_info():
        cpu_threshold_key = "THRESHOLD_CPU_USAGE_WARN_INT"
        mem_threshold_key = "THRESHOLD_MEM_USAGE_WARN_INT"
        disk_threshold_key = "THRESHOLD_DISK_USAGE_WARN_INT"
        email_sender_key = "EMAIL_SENDER"
        operation_time_key = "OPERATION_TIME"
        max_failure_times_key = "MAX_FAILURE_TIMES_INT"
        mem_emergency_threshold_key = "THRESHOLD_MEM_EMERGENCY_SHUTDOWN_INT"
        check_interval_key = "CHECK_INTERVAL_INT"
        return [{
            cpu_threshold_key: 90, mem_threshold_key: 90,
            disk_threshold_key: 90, email_sender_key: 'c@c.com',
            operation_time_key: '8-18', max_failure_times_key: 3,
            mem_emergency_threshold_key: 95, check_interval_key: 15,
            Mc.DB_CONFIGURATION_SERVER: [{Mc.FIELD_SERVER_ID: 1, Mc.FIELD_SERVER_FULL_NAME: "test_server1"}]}]
