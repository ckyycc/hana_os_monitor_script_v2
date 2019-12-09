from unittest import TestCase
from unittest.mock import MagicMock, call, patch

from util import MonitorUtility as Mu
from util import MonitorConst as Mc
from util import InfoType

from agent import DiskMonitor, MemoryMonitor, CPUMonitor, InstanceInfoMonitor, MsgProducerService


class TestAgent(TestCase):
    def setUp(self):
        self.server_id = 1
        self.check_id = "20191125010101001"
        Mu.generate_check_id = MagicMock(return_value=self.check_id)

    def tearDown(self):
        # remove singleton status, otherwise other test cases will be failed as they are share the same class
        MsgProducerService._MsgProducerService__instance = None

    @patch("agent.KafkaProducer")
    def test_disk_monitor(self, mock_producer):
        monitor = DiskMonitor(self.server_id, '/usr/sap', 3600)

        total, free = 1234567890, 34567890

        info = [{Mc.FIELD_USER_NAME: "ck1adm", Mc.FIELD_FOLDER: "folder1", Mc.FIELD_DISK_USAGE_KB: 10000},
                {Mc.FIELD_USER_NAME: "ck2adm", Mc.FIELD_FOLDER: "folder2", Mc.FIELD_DISK_USAGE_KB: 20000},
                {Mc.FIELD_USER_NAME: "ck3adm", Mc.FIELD_FOLDER: "folder3", Mc.FIELD_DISK_USAGE_KB: 30000}]

        # mock os operator
        monitor._os_operator.collect_disk_info = MagicMock(return_value=(total, free))
        monitor._os_operator.get_disk_consumers = MagicMock(return_value=info)

        monitor.monitoring(self.check_id)

        calls = [
            # heart beat
            self.__get_heartbeat_call(InfoType.DISK.value),
            # header
            self.__get_msg_head_call(InfoType.DISK.value),
            # total_free
            call(Mc.TOPIC_MONITORING_INFO,
                 {Mc.FIELD_CHECK_ID: self.check_id, Mc.FIELD_DISK_TOTAL: total,
                  Mc.FIELD_DISK_FREE: free, Mc.MSG_TYPE: InfoType.DISK.value, Mc.FIELD_SERVER_ID: self.server_id}),
            # info1
            call(Mc.TOPIC_MONITORING_INFO,
                 {Mc.FIELD_USER_NAME: "ck1adm", Mc.FIELD_FOLDER: "folder1", Mc.FIELD_DISK_USAGE_KB: 10000,
                  Mc.MSG_TYPE: InfoType.DISK.value, Mc.FIELD_SERVER_ID: self.server_id}),
            # info2
            call(Mc.TOPIC_MONITORING_INFO,
                 {Mc.FIELD_USER_NAME: "ck2adm", Mc.FIELD_FOLDER: "folder2", Mc.FIELD_DISK_USAGE_KB: 20000,
                  Mc.MSG_TYPE: InfoType.DISK.value, Mc.FIELD_SERVER_ID: self.server_id}),
            # info3
            call(Mc.TOPIC_MONITORING_INFO,
                 {Mc.FIELD_USER_NAME: "ck3adm", Mc.FIELD_FOLDER: "folder3", Mc.FIELD_DISK_USAGE_KB: 30000,
                  Mc.MSG_TYPE: InfoType.DISK.value, Mc.FIELD_SERVER_ID: self.server_id}),
            # ending
            self.__get_msg_ending_call(InfoType.DISK.value)

        ]

        mock_producer.return_value.send.assert_has_calls(calls, any_order=False)  # should be sequential

    @patch("agent.KafkaProducer")
    def test_mem_monitor(self, mock_producer):
        monitor = MemoryMonitor(self.server_id, 15)
        total, free = 1000000000, 2500000

        info = [
            {Mc.FIELD_USER_NAME: "ck1adm", Mc.FIELD_PROCESS_COMMAND: "c1", Mc.FIELD_PROCESS_ID: 1001, Mc.FIELD_MEM: 15},
            {Mc.FIELD_USER_NAME: "ck2adm", Mc.FIELD_PROCESS_COMMAND: "c2", Mc.FIELD_PROCESS_ID: 2001, Mc.FIELD_MEM: 25},
            {Mc.FIELD_USER_NAME: "ck3adm", Mc.FIELD_PROCESS_COMMAND: "c3", Mc.FIELD_PROCESS_ID: 3001, Mc.FIELD_MEM: 35}]

        # mock os operator
        monitor._os_operator.collect_mem_info = MagicMock(return_value=(total, free))
        monitor._os_operator.get_mem_consumers = MagicMock(return_value=info)
        monitor.monitoring(self.check_id)

        calls = [
            # heart beat
            self.__get_heartbeat_call(InfoType.MEMORY.value),
            # header
            self.__get_msg_head_call(InfoType.MEMORY.value),
            # total_free
            call(Mc.TOPIC_MONITORING_INFO,
                 {Mc.FIELD_CHECK_ID: self.check_id, Mc.FIELD_MEM_TOTAL: total,
                  Mc.FIELD_MEM_FREE: free, Mc.MSG_TYPE: InfoType.MEMORY.value, Mc.FIELD_SERVER_ID: self.server_id}),
            # info1
            call(Mc.TOPIC_MONITORING_INFO,
                 {Mc.FIELD_USER_NAME: "ck1adm", Mc.FIELD_PROCESS_COMMAND: "c1", Mc.FIELD_PROCESS_ID: 1001,
                  Mc.FIELD_MEM: 15, Mc.MSG_TYPE: InfoType.MEMORY.value, Mc.FIELD_SERVER_ID: self.server_id}),
            # info2
            call(Mc.TOPIC_MONITORING_INFO,
                 {Mc.FIELD_USER_NAME: "ck2adm", Mc.FIELD_PROCESS_COMMAND: "c2", Mc.FIELD_PROCESS_ID: 2001,
                  Mc.FIELD_MEM: 25, Mc.MSG_TYPE: InfoType.MEMORY.value, Mc.FIELD_SERVER_ID: self.server_id}),
            # info3
            call(Mc.TOPIC_MONITORING_INFO,
                 {Mc.FIELD_USER_NAME: "ck3adm", Mc.FIELD_PROCESS_COMMAND: "c3", Mc.FIELD_PROCESS_ID: 3001,
                  Mc.FIELD_MEM: 35, Mc.MSG_TYPE: InfoType.MEMORY.value, Mc.FIELD_SERVER_ID: self.server_id}),
            # ending
            self.__get_msg_ending_call(InfoType.MEMORY.value)
        ]

        mock_producer.return_value.send.assert_has_calls(calls, any_order=False)  # should be sequential

    @patch("agent.KafkaProducer")
    def test_cpu_monitor(self, mock_producer):
        monitor = CPUMonitor(self.server_id, 15)
        num, usage = 512, 78

        info = [
            {Mc.FIELD_USER_NAME: "ck1adm", Mc.FIELD_PROCESS_COMMAND: "c5", Mc.FIELD_PROCESS_ID: 1002, Mc.FIELD_CPU: 18},
            {Mc.FIELD_USER_NAME: "ck2adm", Mc.FIELD_PROCESS_COMMAND: "c6", Mc.FIELD_PROCESS_ID: 2002, Mc.FIELD_CPU: 28},
            {Mc.FIELD_USER_NAME: "ck3adm", Mc.FIELD_PROCESS_COMMAND: "c7", Mc.FIELD_PROCESS_ID: 3002, Mc.FIELD_CPU: 38}]

        # mock os operator
        monitor._os_operator.collect_cpu_info = MagicMock(return_value=(num, usage))
        monitor._os_operator.get_cpu_consumers = MagicMock(return_value=info)
        monitor.monitoring(self.check_id)

        calls = [
            # heart beat
            self.__get_heartbeat_call(InfoType.CPU.value),
            # header
            self.__get_msg_head_call(InfoType.CPU.value),
            # total_free
            call(Mc.TOPIC_MONITORING_INFO,
                 {Mc.FIELD_CHECK_ID: self.check_id, Mc.FIELD_CPU_NUMBER: num, Mc.FIELD_CPU_UTILIZATION: usage,
                  Mc.MSG_TYPE: InfoType.CPU.value, Mc.FIELD_SERVER_ID: self.server_id}),
            # info1
            call(Mc.TOPIC_MONITORING_INFO,
                 {Mc.FIELD_USER_NAME: "ck1adm", Mc.FIELD_PROCESS_COMMAND: "c5", Mc.FIELD_PROCESS_ID: 1002,
                  Mc.FIELD_CPU: 18, Mc.MSG_TYPE: InfoType.CPU.value, Mc.FIELD_SERVER_ID: self.server_id}),
            # info2
            call(Mc.TOPIC_MONITORING_INFO,
                 {Mc.FIELD_USER_NAME: "ck2adm", Mc.FIELD_PROCESS_COMMAND: "c6", Mc.FIELD_PROCESS_ID: 2002,
                  Mc.FIELD_CPU: 28, Mc.MSG_TYPE: InfoType.CPU.value, Mc.FIELD_SERVER_ID: self.server_id}),
            # info3
            call(Mc.TOPIC_MONITORING_INFO,
                 {Mc.FIELD_USER_NAME: "ck3adm", Mc.FIELD_PROCESS_COMMAND: "c7", Mc.FIELD_PROCESS_ID: 3002,
                  Mc.FIELD_CPU: 38, Mc.MSG_TYPE: InfoType.CPU.value, Mc.FIELD_SERVER_ID: self.server_id}),
            # ending
            self.__get_msg_ending_call(InfoType.CPU.value)
        ]

        mock_producer.return_value.send.assert_has_calls(calls, any_order=False)  # should be sequential

    @patch("agent.KafkaProducer")
    def test_instance_monitor(self, mock_producer):
        monitor = InstanceInfoMonitor(self.server_id, 15)

        info = [
            {Mc.FIELD_SID: "CK1", Mc.FIELD_INSTANCE_NO: "00",
             Mc.FIELD_HOST: "server_1", Mc.FIELD_REVISION: "1.00.122.25", Mc.FIELD_EDITION: "Database"},
            {Mc.FIELD_SID: "CK2", Mc.FIELD_INSTANCE_NO: "12",
             Mc.FIELD_HOST: "server_2", Mc.FIELD_REVISION: "2.00.033.00", Mc.FIELD_EDITION: "Cockpit"},
            {Mc.FIELD_SID: "CK3", Mc.FIELD_INSTANCE_NO: "22",
             Mc.FIELD_HOST: "server_3", Mc.FIELD_REVISION: "2.00.044.00", Mc.FIELD_EDITION: "Database"}
        ]
        monitor._os_operator.get_all_hana_instance_info = MagicMock(return_value=info)
        monitor.monitoring(self.check_id)

        calls = [
            # heart beat
            self.__get_heartbeat_call(InfoType.INSTANCE.value),
            # header
            self.__get_msg_head_call(InfoType.INSTANCE.value),
            # overview
            call(Mc.TOPIC_MONITORING_INFO,
                 {Mc.FIELD_CHECK_ID: self.check_id, Mc.MSG_TYPE: InfoType.INSTANCE.value,
                  Mc.FIELD_SERVER_ID: self.server_id}),
            # info1
            call(Mc.TOPIC_MONITORING_INFO,
                 {Mc.FIELD_SID: "CK1", Mc.FIELD_INSTANCE_NO: "00", Mc.FIELD_HOST: "server_1",
                  Mc.FIELD_REVISION: '1.00.122.25', Mc.FIELD_EDITION: 'Database',
                  Mc.MSG_TYPE: InfoType.INSTANCE.value, Mc.FIELD_SERVER_ID: self.server_id}),
            # info2
            call(Mc.TOPIC_MONITORING_INFO,
                 {Mc.FIELD_SID: "CK2", Mc.FIELD_INSTANCE_NO: "12", Mc.FIELD_HOST: "server_2",
                  Mc.FIELD_REVISION: '2.00.033.00', Mc.FIELD_EDITION: 'Cockpit',
                  Mc.MSG_TYPE: InfoType.INSTANCE.value, Mc.FIELD_SERVER_ID: self.server_id}),
            # info3
            call(Mc.TOPIC_MONITORING_INFO,
                 {Mc.FIELD_SID: "CK3", Mc.FIELD_INSTANCE_NO: "22", Mc.FIELD_HOST: "server_3",
                  Mc.FIELD_REVISION: '2.00.044.00', Mc.FIELD_EDITION: 'Database',
                  Mc.MSG_TYPE: InfoType.INSTANCE.value, Mc.FIELD_SERVER_ID: self.server_id}),
            # ending
            self.__get_msg_ending_call(InfoType.INSTANCE.value)
        ]

        mock_producer.return_value.send.assert_has_calls(calls, any_order=False)  # should be sequential

    def __get_heartbeat_call(self, info_type):
        return call(Mc.TOPIC_AGENT_HEARTBEAT,
                    {Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_TYPE: info_type, Mc.MSG_TIME: self.check_id})

    def __get_msg_head_call(self, info_type):
        return call(Mc.TOPIC_MONITORING_INFO,
                    {Mc.MSG_TYPE: info_type, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_HEADER: True})

    def __get_msg_ending_call(self, info_type):
        return call(Mc.TOPIC_MONITORING_INFO,
                    {Mc.MSG_TYPE: info_type, Mc.FIELD_SERVER_ID: self.server_id, Mc.MSG_ENDING: True})
