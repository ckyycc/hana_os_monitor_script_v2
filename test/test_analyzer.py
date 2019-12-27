from unittest import TestCase
from unittest.mock import MagicMock, call, patch

from util import MonitorUtility as Mu
from util import MonitorConst as Mc
from util import InfoType
from analyzer import DataAnalyzer


class TestAnalyzer(TestCase):
    def setUp(self):
        self.server_id = 1
        self.check_id = "20191125010101001"
        Mu.generate_check_id = MagicMock(return_value=self.check_id)

    def test_analyze_disk(self):
        disk_total, disk_free = 1234567890, 34567890

        msg_list = [{Mc.MSG_TYPE: InfoType.DISK.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_HEADER: True},
                    {Mc.FIELD_CHECK_ID: self.check_id, Mc.FIELD_DISK_TOTAL: disk_total, Mc.FIELD_DISK_FREE: disk_free,
                     Mc.MSG_TYPE: InfoType.DISK.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_USER_NAME: "ck1adm", Mc.FIELD_FOLDER: "folder1", Mc.FIELD_DISK_USAGE_KB: 10000,
                     Mc.MSG_TYPE: InfoType.DISK.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_USER_NAME: "ck2adm", Mc.FIELD_FOLDER: "folder2", Mc.FIELD_DISK_USAGE_KB: 20000,
                     Mc.MSG_TYPE: InfoType.DISK.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_USER_NAME: "ck3adm", Mc.FIELD_FOLDER: "folder3", Mc.FIELD_DISK_USAGE_KB: 30000,
                     Mc.MSG_TYPE: InfoType.DISK.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.MSG_TYPE: InfoType.DISK.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_ENDING: True}]

        mock_producer = self.__mock_analyze(msg_list)

        calls = [
            call(Mc.TOPIC_FILTERED_INFO,
                 {Mc.MSG_TYPE: InfoType.DISK.value, Mc.MSG_INFO:
                     {"folder1": {"ck1adm": 10000}, 'folder2': {'ck2adm': 20000}, 'folder3': {'ck3adm': 30000}},
                  Mc.FIELD_CHECK_ID: self.check_id, Mc.FIELD_SERVER_ID: self.server_id, Mc.FIELD_DISK_TOTAL: disk_total,
                  Mc.FIELD_DISK_FREE: disk_free})]

        mock_producer.return_value.send.assert_has_calls(calls, any_order=False)  # should be sequential

    def test_analyze_mem(self):
        mem_total, mem_free = 1000000000, 2500000
        msg_list = [{Mc.MSG_TYPE: InfoType.MEMORY.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_HEADER: True},
                    {Mc.FIELD_CHECK_ID: self.check_id, Mc.FIELD_MEM_TOTAL: mem_total, Mc.FIELD_MEM_FREE: mem_free,
                     Mc.MSG_TYPE: InfoType.MEMORY.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_USER_NAME: "ck1adm", Mc.FIELD_PROCESS_COMMAND: "c1", Mc.FIELD_PROCESS_ID: 1001,
                     Mc.FIELD_MEM: 15, Mc.MSG_TYPE: InfoType.MEMORY.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_USER_NAME: "ck2adm", Mc.FIELD_PROCESS_COMMAND: "c2", Mc.FIELD_PROCESS_ID: 2001,
                     Mc.FIELD_MEM: 25, Mc.MSG_TYPE: InfoType.MEMORY.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_USER_NAME: "ck3adm", Mc.FIELD_PROCESS_COMMAND: "c3", Mc.FIELD_PROCESS_ID: 3001,
                     Mc.FIELD_MEM: 35, Mc.MSG_TYPE: InfoType.MEMORY.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.MSG_TYPE: InfoType.MEMORY.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_ENDING: True}]

        mock_producer = self.__mock_analyze(msg_list)

        calls = [
            call(Mc.TOPIC_FILTERED_INFO,
                 {Mc.MSG_TYPE: InfoType.MEMORY.value, Mc.MSG_INFO: {"ck1adm": 15, 'ck2adm': 25, 'ck3adm': 35},
                  Mc.FIELD_CHECK_ID: self.check_id, Mc.FIELD_SERVER_ID: self.server_id, Mc.FIELD_MEM_TOTAL: mem_total,
                  Mc.FIELD_MEM_FREE: mem_free})]

        mock_producer.return_value.send.assert_has_calls(calls, any_order=False)  # should be sequential

    def test_analyze_cpu(self):
        num, usage = 512, 78

        msg_list = [{Mc.MSG_TYPE: InfoType.CPU.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_HEADER: True},
                    {Mc.FIELD_CHECK_ID: self.check_id, Mc.FIELD_CPU_NUMBER: num, Mc.FIELD_CPU_UTILIZATION: usage,
                     Mc.MSG_TYPE: InfoType.CPU.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_USER_NAME: "ck1adm", Mc.FIELD_PROCESS_COMMAND: "c5", Mc.FIELD_PROCESS_ID: 1002,
                     Mc.FIELD_CPU: 18, Mc.MSG_TYPE: InfoType.CPU.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_USER_NAME: "ck2adm", Mc.FIELD_PROCESS_COMMAND: "c6", Mc.FIELD_PROCESS_ID: 2002,
                     Mc.FIELD_CPU: 28, Mc.MSG_TYPE: InfoType.CPU.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_USER_NAME: "ck3adm", Mc.FIELD_PROCESS_COMMAND: "c7", Mc.FIELD_PROCESS_ID: 3002,
                     Mc.FIELD_CPU: 38, Mc.MSG_TYPE: InfoType.CPU.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.MSG_TYPE: InfoType.CPU.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_ENDING: True}]

        mock_producer = self.__mock_analyze(msg_list)

        calls = [
            call(Mc.TOPIC_FILTERED_INFO,
                 {Mc.MSG_TYPE: InfoType.CPU.value, Mc.MSG_INFO: {"ck1adm": 18, 'ck2adm': 28, 'ck3adm': 38},
                  Mc.FIELD_CHECK_ID: self.check_id, Mc.FIELD_SERVER_ID: self.server_id, Mc.FIELD_CPU_NUMBER: num,
                  Mc.FIELD_CPU_UTILIZATION: usage})]

        mock_producer.return_value.send.assert_has_calls(calls, any_order=False)  # should be sequential

    def test_analyze_instance(self):
        msg_list = [{Mc.MSG_TYPE: InfoType.INSTANCE.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_HEADER: True},
                    {Mc.FIELD_CHECK_ID: self.check_id, Mc.MSG_TYPE: InfoType.INSTANCE.value,
                     Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_SID: "CK1", Mc.FIELD_INSTANCE_NO: "00", Mc.FIELD_HOST: "server_1",
                     Mc.FIELD_REVISION: '1.00.122.25', Mc.FIELD_EDITION: 'Database',
                     Mc.MSG_TYPE: InfoType.INSTANCE.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_SID: "CK2", Mc.FIELD_INSTANCE_NO: "12", Mc.FIELD_HOST: "server_2",
                     Mc.FIELD_REVISION: '2.00.033.00', Mc.FIELD_EDITION: 'Cockpit',
                     Mc.MSG_TYPE: InfoType.INSTANCE.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_SID: "CK3", Mc.FIELD_INSTANCE_NO: "22", Mc.FIELD_HOST: "server_3",
                     Mc.FIELD_REVISION: '2.00.044.00', Mc.FIELD_EDITION: 'Database',
                     Mc.MSG_TYPE: InfoType.INSTANCE.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.MSG_TYPE: InfoType.INSTANCE.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_ENDING: True}]

        mock_producer = self.__mock_analyze(msg_list)

        calls = [
            call(Mc.TOPIC_FILTERED_INFO,
                 {Mc.MSG_TYPE: InfoType.INSTANCE.value, Mc.MSG_INFO:
                     {"CK1": {Mc.FIELD_SID: "CK1", Mc.FIELD_INSTANCE_NO: "00", Mc.FIELD_HOST: "server_1",
                              Mc.FIELD_REVISION: '1.00.122.25', Mc.FIELD_EDITION: 'Database',
                              Mc.FIELD_SERVER_ID: self.server_id},
                      "CK2": {Mc.FIELD_SID: "CK2", Mc.FIELD_INSTANCE_NO: "12", Mc.FIELD_HOST: "server_2",
                              Mc.FIELD_REVISION: '2.00.033.00', Mc.FIELD_EDITION: 'Cockpit',
                              Mc.FIELD_SERVER_ID: self.server_id},
                      "CK3": {Mc.FIELD_SID: "CK3", Mc.FIELD_INSTANCE_NO: "22", Mc.FIELD_HOST: "server_3",
                              Mc.FIELD_REVISION: '2.00.044.00', Mc.FIELD_EDITION: 'Database',
                              Mc.FIELD_SERVER_ID: self.server_id}},
                  Mc.FIELD_CHECK_ID: self.check_id, Mc.FIELD_SERVER_ID: self.server_id})]

        mock_producer.return_value.send.assert_has_calls(calls, any_order=False)  # should be sequential

    def test_analyze_mix(self):
        num, usage = 512, 78
        mem_total, mem_free = 1000000000, 2500000
        disk_total, disk_free = 1234567890, 34567890

        msg_list = [{Mc.MSG_TYPE: InfoType.INSTANCE.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_HEADER: True},
                    {Mc.FIELD_CHECK_ID: self.check_id, Mc.MSG_TYPE: InfoType.INSTANCE.value,
                     Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_SID: "CK1", Mc.FIELD_INSTANCE_NO: "00", Mc.FIELD_HOST: "server_1",
                     Mc.FIELD_REVISION: '1.00.122.25', Mc.FIELD_EDITION: 'Database',
                     Mc.MSG_TYPE: InfoType.INSTANCE.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.MSG_TYPE: InfoType.CPU.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_HEADER: True},
                    {Mc.FIELD_CHECK_ID: self.check_id, Mc.FIELD_CPU_NUMBER: num, Mc.FIELD_CPU_UTILIZATION: usage,
                     Mc.MSG_TYPE: InfoType.CPU.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_USER_NAME: "ck1adm", Mc.FIELD_PROCESS_COMMAND: "c5", Mc.FIELD_PROCESS_ID: 1002,
                     Mc.FIELD_CPU: 18, Mc.MSG_TYPE: InfoType.CPU.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_SID: "CK2", Mc.FIELD_INSTANCE_NO: "12", Mc.FIELD_HOST: "server_2",
                     Mc.FIELD_REVISION: '2.00.033.00', Mc.FIELD_EDITION: 'Cockpit',
                     Mc.MSG_TYPE: InfoType.INSTANCE.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.MSG_TYPE: InfoType.MEMORY.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_HEADER: True},
                    {Mc.FIELD_CHECK_ID: self.check_id, Mc.FIELD_MEM_TOTAL: mem_total, Mc.FIELD_MEM_FREE: mem_free,
                     Mc.MSG_TYPE: InfoType.MEMORY.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_USER_NAME: "ck1adm", Mc.FIELD_PROCESS_COMMAND: "c1", Mc.FIELD_PROCESS_ID: 1001,
                     Mc.FIELD_MEM: 15, Mc.MSG_TYPE: InfoType.MEMORY.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.MSG_TYPE: InfoType.DISK.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_HEADER: True},
                    {Mc.FIELD_CHECK_ID: self.check_id, Mc.FIELD_DISK_TOTAL: disk_total, Mc.FIELD_DISK_FREE: disk_free,
                     Mc.MSG_TYPE: InfoType.DISK.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_USER_NAME: "ck2adm", Mc.FIELD_PROCESS_COMMAND: "c2", Mc.FIELD_PROCESS_ID: 2001,
                     Mc.FIELD_MEM: 25, Mc.MSG_TYPE: InfoType.MEMORY.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_USER_NAME: "ck3adm", Mc.FIELD_PROCESS_COMMAND: "c3", Mc.FIELD_PROCESS_ID: 3001,
                     Mc.FIELD_MEM: 35, Mc.MSG_TYPE: InfoType.MEMORY.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_USER_NAME: "ck2adm", Mc.FIELD_PROCESS_COMMAND: "c6", Mc.FIELD_PROCESS_ID: 2002,
                     Mc.FIELD_CPU: 28, Mc.MSG_TYPE: InfoType.CPU.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_USER_NAME: "ck3adm", Mc.FIELD_PROCESS_COMMAND: "c7", Mc.FIELD_PROCESS_ID: 3002,
                     Mc.FIELD_CPU: 38, Mc.MSG_TYPE: InfoType.CPU.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.MSG_TYPE: InfoType.CPU.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_ENDING: True},
                    {Mc.FIELD_USER_NAME: "ck1adm", Mc.FIELD_FOLDER: "folder1", Mc.FIELD_DISK_USAGE_KB: 10000,
                     Mc.MSG_TYPE: InfoType.DISK.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_USER_NAME: "ck2adm", Mc.FIELD_FOLDER: "folder2", Mc.FIELD_DISK_USAGE_KB: 20000,
                     Mc.MSG_TYPE: InfoType.DISK.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_USER_NAME: "ck3adm", Mc.FIELD_FOLDER: "folder3", Mc.FIELD_DISK_USAGE_KB: 30000,
                     Mc.MSG_TYPE: InfoType.DISK.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.MSG_TYPE: InfoType.DISK.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_ENDING: True},
                    {Mc.FIELD_SID: "CK3", Mc.FIELD_INSTANCE_NO: "22", Mc.FIELD_HOST: "server_3",
                     Mc.FIELD_REVISION: '2.00.044.00', Mc.FIELD_EDITION: 'Database',
                     Mc.MSG_TYPE: InfoType.INSTANCE.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.MSG_TYPE: InfoType.INSTANCE.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_ENDING: True},
                    {Mc.MSG_TYPE: InfoType.MEMORY.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_ENDING: True}]

        mock_producer = self.__mock_analyze(msg_list)

        calls = [
            call(Mc.TOPIC_FILTERED_INFO,
                 {Mc.MSG_TYPE: InfoType.CPU.value, Mc.MSG_INFO: {"ck1adm": 18, 'ck2adm': 28, 'ck3adm': 38},
                  Mc.FIELD_CHECK_ID: self.check_id, Mc.FIELD_SERVER_ID: self.server_id, Mc.FIELD_CPU_NUMBER: num,
                  Mc.FIELD_CPU_UTILIZATION: usage}),
            call(Mc.TOPIC_FILTERED_INFO, {Mc.MSG_TYPE: InfoType.DISK.value,
                                          Mc.MSG_INFO: {"folder1": {"ck1adm": 10000}, 'folder2': {'ck2adm': 20000},
                                                        'folder3': {'ck3adm': 30000}}, Mc.FIELD_CHECK_ID: self.check_id,
                                          Mc.FIELD_SERVER_ID: self.server_id, Mc.FIELD_DISK_TOTAL: disk_total,
                                          Mc.FIELD_DISK_FREE: disk_free}),
            call(Mc.TOPIC_FILTERED_INFO, {Mc.MSG_TYPE: InfoType.INSTANCE.value, Mc.MSG_INFO: {
                "CK1": {Mc.FIELD_SID: "CK1", Mc.FIELD_INSTANCE_NO: "00", Mc.FIELD_HOST: "server_1",
                        Mc.FIELD_REVISION: '1.00.122.25', Mc.FIELD_EDITION: 'Database',
                        Mc.FIELD_SERVER_ID: self.server_id},
                "CK2": {Mc.FIELD_SID: "CK2", Mc.FIELD_INSTANCE_NO: "12", Mc.FIELD_HOST: "server_2",
                        Mc.FIELD_REVISION: '2.00.033.00', Mc.FIELD_EDITION: 'Cockpit',
                        Mc.FIELD_SERVER_ID: self.server_id},
                "CK3": {Mc.FIELD_SID: "CK3", Mc.FIELD_INSTANCE_NO: "22", Mc.FIELD_HOST: "server_3",
                        Mc.FIELD_REVISION: '2.00.044.00', Mc.FIELD_EDITION: 'Database',
                        Mc.FIELD_SERVER_ID: self.server_id}}, Mc.FIELD_CHECK_ID: self.check_id,
                                          Mc.FIELD_SERVER_ID: self.server_id}),
            call(Mc.TOPIC_FILTERED_INFO,
                 {Mc.MSG_TYPE: InfoType.MEMORY.value, Mc.MSG_INFO: {"ck1adm": 15, 'ck2adm': 25, 'ck3adm': 35},
                  Mc.FIELD_CHECK_ID: self.check_id, Mc.FIELD_SERVER_ID: self.server_id, Mc.FIELD_MEM_TOTAL: mem_total,
                  Mc.FIELD_MEM_FREE: mem_free})
        ]

        mock_producer.return_value.send.assert_has_calls(calls, any_order=False)  # should be sequential

    def test_analyze_mix_abandon_if_no_ending(self):
        num, usage = 512, 78
        mem_total, mem_free = 1000000000, 2500000
        disk_total, disk_free = 1234567890, 34567890

        msg_list = [{Mc.MSG_TYPE: InfoType.INSTANCE.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_HEADER: True},
                    {Mc.FIELD_CHECK_ID: self.check_id, Mc.MSG_TYPE: InfoType.INSTANCE.value,
                     Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_SID: "CK1", Mc.FIELD_INSTANCE_NO: "00", Mc.FIELD_HOST: "server_1",
                     Mc.FIELD_REVISION: '1.00.122.25', Mc.FIELD_EDITION: 'Database',
                     Mc.MSG_TYPE: InfoType.INSTANCE.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.MSG_TYPE: InfoType.CPU.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_HEADER: True},
                    {Mc.FIELD_CHECK_ID: self.check_id, Mc.FIELD_CPU_NUMBER: num, Mc.FIELD_CPU_UTILIZATION: usage,
                     Mc.MSG_TYPE: InfoType.CPU.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_USER_NAME: "ck1adm", Mc.FIELD_PROCESS_COMMAND: "c5", Mc.FIELD_PROCESS_ID: 1002,
                     Mc.FIELD_CPU: 18, Mc.MSG_TYPE: InfoType.CPU.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_SID: "CK2", Mc.FIELD_INSTANCE_NO: "12", Mc.FIELD_HOST: "server_2",
                     Mc.FIELD_REVISION: '2.00.033.00', Mc.FIELD_EDITION: 'Cockpit',
                     Mc.MSG_TYPE: InfoType.INSTANCE.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.MSG_TYPE: InfoType.MEMORY.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_HEADER: True},
                    {Mc.FIELD_CHECK_ID: self.check_id, Mc.FIELD_MEM_TOTAL: mem_total, Mc.FIELD_MEM_FREE: mem_free,
                     Mc.MSG_TYPE: InfoType.MEMORY.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_USER_NAME: "ck1adm", Mc.FIELD_PROCESS_COMMAND: "c1", Mc.FIELD_PROCESS_ID: 1001,
                     Mc.FIELD_MEM: 15, Mc.MSG_TYPE: InfoType.MEMORY.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.MSG_TYPE: InfoType.DISK.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_HEADER: True},
                    {Mc.FIELD_CHECK_ID: self.check_id, Mc.FIELD_DISK_TOTAL: disk_total, Mc.FIELD_DISK_FREE: disk_free,
                     Mc.MSG_TYPE: InfoType.DISK.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_USER_NAME: "ck2adm", Mc.FIELD_PROCESS_COMMAND: "c2", Mc.FIELD_PROCESS_ID: 2001,
                     Mc.FIELD_MEM: 25, Mc.MSG_TYPE: InfoType.MEMORY.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_USER_NAME: "ck3adm", Mc.FIELD_PROCESS_COMMAND: "c3", Mc.FIELD_PROCESS_ID: 3001,
                     Mc.FIELD_MEM: 35, Mc.MSG_TYPE: InfoType.MEMORY.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_USER_NAME: "ck2adm", Mc.FIELD_PROCESS_COMMAND: "c6", Mc.FIELD_PROCESS_ID: 2002,
                     Mc.FIELD_CPU: 28, Mc.MSG_TYPE: InfoType.CPU.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_USER_NAME: "ck3adm", Mc.FIELD_PROCESS_COMMAND: "c7", Mc.FIELD_PROCESS_ID: 3002,
                     Mc.FIELD_CPU: 38, Mc.MSG_TYPE: InfoType.CPU.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.MSG_TYPE: InfoType.CPU.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_ENDING: True},
                    {Mc.FIELD_USER_NAME: "ck1adm", Mc.FIELD_FOLDER: "folder1", Mc.FIELD_DISK_USAGE_KB: 10000,
                     Mc.MSG_TYPE: InfoType.DISK.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_USER_NAME: "ck2adm", Mc.FIELD_FOLDER: "folder2", Mc.FIELD_DISK_USAGE_KB: 20000,
                     Mc.MSG_TYPE: InfoType.DISK.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_USER_NAME: "ck3adm", Mc.FIELD_FOLDER: "folder3", Mc.FIELD_DISK_USAGE_KB: 30000,
                     Mc.MSG_TYPE: InfoType.DISK.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.MSG_TYPE: InfoType.DISK.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_ENDING: True},
                    {Mc.FIELD_SID: "CK3", Mc.FIELD_INSTANCE_NO: "22", Mc.FIELD_HOST: "server_3",
                     Mc.FIELD_REVISION: '2.00.044.00', Mc.FIELD_EDITION: 'Database',
                     Mc.MSG_TYPE: InfoType.INSTANCE.value, Mc.FIELD_SERVER_ID: self.server_id},
                    #  remove ending of instance
                    #  {Mc.MSG_TYPE: InfoType.INSTANCE.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_ENDING: True},
                    {Mc.MSG_TYPE: InfoType.MEMORY.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_ENDING: True}]

        mock_producer = self.__mock_analyze(msg_list)

        calls = [
            call(Mc.TOPIC_FILTERED_INFO,
                 {Mc.MSG_TYPE: InfoType.CPU.value, Mc.MSG_INFO: {"ck1adm": 18, 'ck2adm': 28, 'ck3adm': 38},
                  Mc.FIELD_CHECK_ID: self.check_id, Mc.FIELD_SERVER_ID: self.server_id, Mc.FIELD_CPU_NUMBER: num,
                  Mc.FIELD_CPU_UTILIZATION: usage}),
            call(Mc.TOPIC_FILTERED_INFO, {Mc.MSG_TYPE: InfoType.DISK.value,
                                          Mc.MSG_INFO: {"folder1": {"ck1adm": 10000}, 'folder2': {'ck2adm': 20000},
                                                        'folder3': {'ck3adm': 30000}}, Mc.FIELD_CHECK_ID: self.check_id,
                                          Mc.FIELD_SERVER_ID: self.server_id, Mc.FIELD_DISK_TOTAL: disk_total,
                                          Mc.FIELD_DISK_FREE: disk_free}),
            call(Mc.TOPIC_FILTERED_INFO,
                 {Mc.MSG_TYPE: InfoType.MEMORY.value, Mc.MSG_INFO: {"ck1adm": 15, 'ck2adm': 25, 'ck3adm': 35},
                  Mc.FIELD_CHECK_ID: self.check_id, Mc.FIELD_SERVER_ID: self.server_id, Mc.FIELD_MEM_TOTAL: mem_total,
                  Mc.FIELD_MEM_FREE: mem_free})
        ]

        mock_producer.return_value.send.assert_has_calls(calls, any_order=False)  # should be sequential

    def test_analyze_mix_abandon_first_msg_if_second_comes_before_ending_of_first_one(self):
        num, usage = 512, 78
        mem_total, mem_free = 1000000000, 2500000
        disk_total, disk_free = 1234567890, 34567890

        msg_list = [{Mc.MSG_TYPE: InfoType.INSTANCE.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_HEADER: True},
                    {Mc.FIELD_CHECK_ID: self.check_id, Mc.MSG_TYPE: InfoType.INSTANCE.value,
                     Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_SID: "CK1", Mc.FIELD_INSTANCE_NO: "00", Mc.FIELD_HOST: "server_1",
                     Mc.FIELD_REVISION: '1.00.122.25', Mc.FIELD_EDITION: 'Database',
                     Mc.MSG_TYPE: InfoType.INSTANCE.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.MSG_TYPE: InfoType.CPU.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_HEADER: True},
                    {Mc.FIELD_CHECK_ID: self.check_id, Mc.FIELD_CPU_NUMBER: num, Mc.FIELD_CPU_UTILIZATION: usage,
                     Mc.MSG_TYPE: InfoType.CPU.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_USER_NAME: "ck1adm", Mc.FIELD_PROCESS_COMMAND: "c5", Mc.FIELD_PROCESS_ID: 1002,
                     Mc.FIELD_CPU: 18, Mc.MSG_TYPE: InfoType.CPU.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_SID: "CK2", Mc.FIELD_INSTANCE_NO: "12", Mc.FIELD_HOST: "server_2",
                     Mc.FIELD_REVISION: '2.00.033.00', Mc.FIELD_EDITION: 'Cockpit',
                     Mc.MSG_TYPE: InfoType.INSTANCE.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.MSG_TYPE: InfoType.MEMORY.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_HEADER: True},
                    {Mc.FIELD_CHECK_ID: self.check_id, Mc.FIELD_MEM_TOTAL: mem_total, Mc.FIELD_MEM_FREE: mem_free,
                     Mc.MSG_TYPE: InfoType.MEMORY.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_USER_NAME: "ck1adm", Mc.FIELD_PROCESS_COMMAND: "c1", Mc.FIELD_PROCESS_ID: 1001,
                     Mc.FIELD_MEM: 15, Mc.MSG_TYPE: InfoType.MEMORY.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.MSG_TYPE: InfoType.DISK.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_HEADER: True},
                    {Mc.FIELD_CHECK_ID: self.check_id, Mc.FIELD_DISK_TOTAL: disk_total, Mc.FIELD_DISK_FREE: disk_free,
                     Mc.MSG_TYPE: InfoType.DISK.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_USER_NAME: "ck2adm", Mc.FIELD_PROCESS_COMMAND: "c2", Mc.FIELD_PROCESS_ID: 2001,
                     Mc.FIELD_MEM: 25, Mc.MSG_TYPE: InfoType.MEMORY.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_USER_NAME: "ck3adm", Mc.FIELD_PROCESS_COMMAND: "c3", Mc.FIELD_PROCESS_ID: 3001,
                     Mc.FIELD_MEM: 35, Mc.MSG_TYPE: InfoType.MEMORY.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_USER_NAME: "ck2adm", Mc.FIELD_PROCESS_COMMAND: "c6", Mc.FIELD_PROCESS_ID: 2002,
                     Mc.FIELD_CPU: 28, Mc.MSG_TYPE: InfoType.CPU.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_USER_NAME: "ck3adm", Mc.FIELD_PROCESS_COMMAND: "c7", Mc.FIELD_PROCESS_ID: 3002,
                     Mc.FIELD_CPU: 38, Mc.MSG_TYPE: InfoType.CPU.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.MSG_TYPE: InfoType.CPU.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_ENDING: True},
                    {Mc.FIELD_USER_NAME: "ck1adm", Mc.FIELD_FOLDER: "folder1", Mc.FIELD_DISK_USAGE_KB: 10000,
                     Mc.MSG_TYPE: InfoType.DISK.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_USER_NAME: "ck2adm", Mc.FIELD_FOLDER: "folder2", Mc.FIELD_DISK_USAGE_KB: 20000,
                     Mc.MSG_TYPE: InfoType.DISK.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_USER_NAME: "ck3adm", Mc.FIELD_FOLDER: "folder3", Mc.FIELD_DISK_USAGE_KB: 30000,
                     Mc.MSG_TYPE: InfoType.DISK.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.MSG_TYPE: InfoType.DISK.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_ENDING: True},
                    {Mc.FIELD_SID: "CK3", Mc.FIELD_INSTANCE_NO: "22", Mc.FIELD_HOST: "server_3",
                     Mc.FIELD_REVISION: '2.00.044.00', Mc.FIELD_EDITION: 'Database',
                     Mc.MSG_TYPE: InfoType.INSTANCE.value, Mc.FIELD_SERVER_ID: self.server_id},
                    #  remove ending of instance
                    #  {Mc.MSG_TYPE: InfoType.INSTANCE.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_ENDING: True},
                    #  second one comes
                    {Mc.MSG_TYPE: InfoType.INSTANCE.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_HEADER: True},
                    {Mc.FIELD_CHECK_ID: self.check_id, Mc.MSG_TYPE: InfoType.INSTANCE.value,
                     Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.FIELD_SID: "CK5", Mc.FIELD_INSTANCE_NO: "05", Mc.FIELD_HOST: "server_1",
                     Mc.FIELD_REVISION: '1.00.122.25', Mc.FIELD_EDITION: 'Database',
                     Mc.MSG_TYPE: InfoType.INSTANCE.value, Mc.FIELD_SERVER_ID: self.server_id},
                    {Mc.MSG_TYPE: InfoType.INSTANCE.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_ENDING: True},
                    {Mc.MSG_TYPE: InfoType.MEMORY.value, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_ENDING: True}]

        mock_producer = self.__mock_analyze(msg_list)

        calls = [
            call(Mc.TOPIC_FILTERED_INFO,
                 {Mc.MSG_TYPE: InfoType.CPU.value, Mc.MSG_INFO: {"ck1adm": 18, 'ck2adm': 28, 'ck3adm': 38},
                  Mc.FIELD_CHECK_ID: self.check_id, Mc.FIELD_SERVER_ID: self.server_id, Mc.FIELD_CPU_NUMBER: num,
                  Mc.FIELD_CPU_UTILIZATION: usage}),
            call(Mc.TOPIC_FILTERED_INFO, {Mc.MSG_TYPE: InfoType.DISK.value,
                                          Mc.MSG_INFO: {"folder1": {"ck1adm": 10000}, 'folder2': {'ck2adm': 20000},
                                                        'folder3': {'ck3adm': 30000}}, Mc.FIELD_CHECK_ID: self.check_id,
                                          Mc.FIELD_SERVER_ID: self.server_id, Mc.FIELD_DISK_TOTAL: disk_total,
                                          Mc.FIELD_DISK_FREE: disk_free}),

            call(Mc.TOPIC_FILTERED_INFO,
                 {Mc.MSG_TYPE: InfoType.INSTANCE.value, Mc.MSG_INFO:
                     {"CK5": {Mc.FIELD_SID: "CK5", Mc.FIELD_INSTANCE_NO: "05", Mc.FIELD_HOST: "server_1",
                              Mc.FIELD_REVISION: '1.00.122.25', Mc.FIELD_EDITION: 'Database',
                              Mc.FIELD_SERVER_ID: self.server_id}},
                  Mc.FIELD_CHECK_ID: self.check_id, Mc.FIELD_SERVER_ID: self.server_id}),
            call(Mc.TOPIC_FILTERED_INFO,
                 {Mc.MSG_TYPE: InfoType.MEMORY.value, Mc.MSG_INFO: {"ck1adm": 15, 'ck2adm': 25, 'ck3adm': 35},
                  Mc.FIELD_CHECK_ID: self.check_id, Mc.FIELD_SERVER_ID: self.server_id, Mc.FIELD_MEM_TOTAL: mem_total,
                  Mc.FIELD_MEM_FREE: mem_free})
        ]

        mock_producer.return_value.send.assert_has_calls(calls, any_order=False)  # should be sequential

    @patch("util.KafkaProducer")
    def __mock_analyze(self, msg_list, mock_producer):
        infos = TestAnalyzer.__get_mock_msg_list(msg_list)
        analyzer = DataAnalyzer()
        analyzer._DataAnalyzer__analyze(infos)
        return mock_producer

    @staticmethod
    def __get_mock_msg_list(msg_list):
        infos = []
        for msg in msg_list:
            info = lambda: None
            setattr(info, "value", {})
            info.value = msg
            infos.append(info)
        return infos
