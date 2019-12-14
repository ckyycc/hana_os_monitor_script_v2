import json
from threading import Thread
from util import MonitorUtility as Mu
from util import MonitorConst as Mc
from kafka import KafkaConsumer
from kafka import KafkaProducer
from util import InfoType
from abc import ABC, abstractmethod


class DataAnalyzer(Thread):
    """Data analyzer for those un-aggregated data.
    It consumes the un-aggregated message from agent and produce aggregated message for db_operator
    """
    def __init__(self):
        super().__init__()
        self.__logger = Mu.get_logger(Mc.LOGGER_MONITOR_ANALYZER)
        self.__producer = KafkaProducer(
            bootstrap_servers=["{0}:{1}".format(Mc.get_kafka_server(), Mc.get_kafka_port())],
            value_serializer=lambda v: json.dumps(v).encode('ascii'))
        self.__topic = Mc.TOPIC_FILTERED_INFO
        self.__mem_info_analyzer = DataAnalyzer.__MemoryInfoAnalyzer()
        self.__cpu_info_analyzer = DataAnalyzer.__CPUInfoAnalyzer()
        self.__disk_info_analyzer = DataAnalyzer.__DiskInfoAnalyzer()
        self.__instance_info_analyzer = DataAnalyzer.__InstanceInfoAnalyzer()

    def __analyze(self, consumer):
        """
        process all the un-aggregated data from agent, produce aggregated data to db-operator
        :param consumer: kafka consumer
        """
        # info structure is { info_type : {server_id : info_detail}}
        # eg : { "memory" : {1: info_detail, 2; info_detail} , "disk" : {3: info_detail : 7: info_detail} }
        info = {}
        # start flag for recording all statuses for different resources and server Ids
        # eg : { "memory" : { 1 : True, 8 : False} , "disk" : { 1 : True, 7 : False } }
        start_flag = {}
        for msg in consumer:
            try:
                if msg and msg.value:
                    message = msg.value
                    server_id = message[Mc.FIELD_SERVER_ID]
                    info_analyzer = self.__get_info_analyzer(message)
                    if server_id is not None and info_analyzer is not None:
                        Mu.log_debug(self.__logger, message)
                        info_type = info_analyzer.type()
                        if DataAnalyzer.__is_header(message):
                            # init the value, if previous ending is lost, all the previous messages with
                            # same type and server id will be abandoned
                            if info_type not in start_flag:
                                start_flag[info_type] = {}
                            start_flag[info_type][server_id] = True
                            # init the info for the specific type
                            if info_type.value not in info:
                                info[info_type.value] = {}
                            info[info_type.value][server_id] = {Mc.MSG_TYPE: info_type.value, Mc.MSG_INFO: {}}
                        elif start_flag.get(info_type, {}).get(server_id, False) and DataAnalyzer.__is_ending(message):
                            # done, analyze the message and put filtered message to queue
                            self.__producer.send(self.__topic, info[info_type.value][server_id])
                            self.__producer.flush()
                            Mu.log_debug(
                                self.__logger, "Filtered message {0} was sent".format(info[info_type.value][server_id]))

                            # reset the start flag and value
                            start_flag[info_type][server_id] = False
                            info[info_type.value][server_id] = {}
                        elif start_flag.get(info_type, {}).get(server_id, False):
                            # only process after get the header
                            DataAnalyzer.__process(info_analyzer, message, info[info_type.value][server_id])
            except Exception as ex:
                Mu.log_warning_exc(self.__logger, "Error occurred when analyzing message, Error: {0}".format(ex))

    @staticmethod
    def __is_header(info):
        return info.get(Mc.MSG_HEADER, False) if info else False

    @staticmethod
    def __is_ending(info):
        return info.get(Mc.MSG_ENDING, False) if info else False

    @staticmethod
    def __process(info_analyzer, message, info):
        """processing the analysis"""
        info_analyzer.process(message, info)

    def __get_info_analyzer(self, info):
        """get the info analyzer for different resource base on the resource type"""
        switcher = {
            InfoType.MEMORY.value: self.__mem_info_analyzer,
            InfoType.CPU.value: self.__cpu_info_analyzer,
            InfoType.DISK.value: self.__disk_info_analyzer,
            InfoType.INSTANCE.value: self.__instance_info_analyzer
        }
        return switcher.get(info.get(Mc.MSG_TYPE, "N/A"), None) if info else None

    def run(self):
        """run the thread"""
        while True:
            consumer = KafkaConsumer(Mc.TOPIC_MONITORING_INFO,
                                     group_id=Mc.MONITOR_GROUP_ID,
                                     bootstrap_servers=["{0}:{1}".format(Mc.get_kafka_server(), Mc.get_kafka_port())],
                                     value_deserializer=lambda m: json.loads(m.decode('ascii')))
            self.__analyze(consumer)
            Mu.log_warning(self.__logger, "Topic is empty or connection is lost. Trying to reconnect...")

    class UnAggregatedDataAnalyzer(ABC):
        """Abstract inner class for the analyzer of different resources"""
        @abstractmethod
        def process(self, message, info):
            """process the un-aggregated message and update info"""
            pass

        @abstractmethod
        def type(self):
            """get the related type of data"""
            pass

    class __MemoryInfoAnalyzer(UnAggregatedDataAnalyzer):
        """Inner class for the analyzer of memory"""
        def process(self, message, info):
            """process the un-aggregated message and update info"""
            # update check id
            if Mc.FIELD_CHECK_ID not in info and Mc.FIELD_CHECK_ID in message:
                info[Mc.FIELD_CHECK_ID] = message[Mc.FIELD_CHECK_ID]

            # update server id
            if Mc.FIELD_SERVER_ID not in info and Mc.FIELD_SERVER_ID in message:
                info[Mc.FIELD_SERVER_ID] = message[Mc.FIELD_SERVER_ID]

            # update total and free
            if Mc.FIELD_MEM_TOTAL in message and Mc.FIELD_MEM_FREE in message and \
                    (Mc.FIELD_MEM_TOTAL not in info or Mc.FIELD_MEM_FREE not in info):
                info[Mc.FIELD_MEM_TOTAL] = message.get(Mc.FIELD_MEM_TOTAL, -1)
                info[Mc.FIELD_MEM_FREE] = message.get(Mc.FIELD_MEM_FREE, -1)
            else:
                # get user name
                user = message.get(Mc.FIELD_USER_NAME, None)
                # calc total usage
                if user:
                    info[Mc.MSG_INFO][user] = info[Mc.MSG_INFO].get(user, 0.0) + float(message.get(Mc.FIELD_MEM, 0))

        def type(self):
            return InfoType.MEMORY

    class __DiskInfoAnalyzer(UnAggregatedDataAnalyzer):
        """Inner class for the analyzer of Disk"""
        def process(self, message, info):
            """process the un-aggregated message and update info"""

            # update check id
            if Mc.FIELD_CHECK_ID not in info and Mc.FIELD_CHECK_ID in message:
                info[Mc.FIELD_CHECK_ID] = message[Mc.FIELD_CHECK_ID]

            # update server id
            if Mc.FIELD_SERVER_ID not in info and Mc.FIELD_SERVER_ID in message:
                info[Mc.FIELD_SERVER_ID] = message[Mc.FIELD_SERVER_ID]

            # update total and free
            if Mc.FIELD_DISK_TOTAL in message and Mc.FIELD_DISK_FREE in message and \
                    (Mc.FIELD_DISK_TOTAL not in info or Mc.FIELD_DISK_FREE not in info):
                info[Mc.FIELD_DISK_TOTAL] = message.get(Mc.FIELD_DISK_TOTAL, -1)
                info[Mc.FIELD_DISK_FREE] = message.get(Mc.FIELD_DISK_FREE, -1)
            else:
                # get user name
                user = message.get(Mc.FIELD_USER_NAME, "N/A")
                # get folder
                folder = message.get(Mc.FIELD_FOLDER, "N/A")
                # calc total usage
                if folder:
                    info[Mc.MSG_INFO][folder] = {user: message.get(Mc.FIELD_DISK_USAGE_KB, 0)}

        def type(self):
            return InfoType.DISK

    class __CPUInfoAnalyzer(UnAggregatedDataAnalyzer):
        """Inner class for the analyzer of CPU"""
        def process(self, message, info):
            """process the un-aggregated message and update info"""
            # update check id
            if Mc.FIELD_CHECK_ID not in info and Mc.FIELD_CHECK_ID in message:
                info[Mc.FIELD_CHECK_ID] = message[Mc.FIELD_CHECK_ID]

            # update server id
            if Mc.FIELD_SERVER_ID not in info and Mc.FIELD_SERVER_ID in message:
                info[Mc.FIELD_SERVER_ID] = message[Mc.FIELD_SERVER_ID]

            # update total and free
            if Mc.FIELD_CPU_NUMBER in message and Mc.FIELD_CPU_UTILIZATION in message and \
                    (Mc.FIELD_CPU_NUMBER not in info or Mc.FIELD_CPU_UTILIZATION not in info):
                info[Mc.FIELD_CPU_NUMBER] = message.get(Mc.FIELD_CPU_NUMBER, -1)
                info[Mc.FIELD_CPU_UTILIZATION] = message.get(Mc.FIELD_CPU_UTILIZATION, -1)
            else:
                # get user name
                user = message.get(Mc.FIELD_USER_NAME, None)
                # calc total usage
                if user:
                    info[Mc.MSG_INFO][user] = info[Mc.MSG_INFO].get(user, 0.0) + float(message.get(Mc.FIELD_CPU, 0))

        def type(self):
            return InfoType.CPU

    class __InstanceInfoAnalyzer(UnAggregatedDataAnalyzer):
        def process(self, message, info):
            """process the un-aggregated message and update info"""
            # update check id
            if Mc.FIELD_CHECK_ID not in info and Mc.FIELD_CHECK_ID in message:
                info[Mc.FIELD_CHECK_ID] = message[Mc.FIELD_CHECK_ID]

            # update server id
            if Mc.FIELD_SERVER_ID not in info and Mc.FIELD_SERVER_ID in message:
                info[Mc.FIELD_SERVER_ID] = message[Mc.FIELD_SERVER_ID]

            # # get user name
            # user = message.get(Mc.FIELD_USER_NAME, None)
            # # calc total usage
            # if user:
            #     info["info"][user] = info["info"].get(user, 0) + float(message.get(Mc.FIELD_CPU_UTILIZATION, 0))
            # get sid
            sid = message.get(Mc.FIELD_SID)
            if sid:
                message.pop(Mc.MSG_TYPE, None)  # remove type
                info[Mc.MSG_INFO][sid] = message

        def type(self):
            return InfoType.INSTANCE


if __name__ == '__main__':
    DataAnalyzer().start()
