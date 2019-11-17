import json

from errors import MonitorDBOpError

import threading
import time

from util import MonitorUtility as Mu
from util import MonitorConst as Mc
from operation.db_operations import HANAMonitorDAO

from kafka import KafkaProducer
from kafka import KafkaConsumer

from kafka.errors import KafkaError


class ConfigMgr(threading.Thread):
    def __init__(self):
        super().__init__()
        self.__db_operator = HANAServerDBOperatorService.instance()
        # TODO: kafka address might be changed, needs to check during monitoring, or config it in configuration.ini
        self.__producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                        value_serializer=lambda v: json.dumps(v).encode('ascii'))
        self.__topic = Mc.TOPIC_CONFIGURATION
        self.__configs = {}

    def __get_interval_time(self):
        return self.__configs.get("CHECK_INTERVAL_INT", 600)  # default 600 seconds

    def __get_configurations(self):
        return self.__db_operator.retreive_configurations()

    def __monitoring_configurations(self):
        configs = self.__get_configurations()
        updated_configs = {}
        for key, value in configs.items():
            if value != self.__configs.get(key, None):
                updated_configs[key] = value
                self.__configs[key] = value

        if updated_configs:
            self.__producer.send(self.__topic, updated_configs)

    def run(self):
        """run the thread"""
        while True:
            self.__monitoring_configurations()
            time.sleep(self.__get_interval_time())


class MonitorCoordinator(threading.Thread):
    def __init__(self):
        super().__init__()
        self.__configs = {}

    def __coordinating_monitors(self, consumer):
        for msg in consumer:
            if self.__update_configs(msg.value):
                # restart all agents
                print("restarting....")
            else:
                continue

    def __update_configs(self, config):
        flag = False

        if not config:
            return flag

        for key, value in config.items():
            if value != self.__configs.get(key, None):
                self.__configs[key] = value
                flag = True
        return flag

    def run(self):
        """run the thread"""
        while True:
            consumer = KafkaConsumer(Mc.TOPIC_CONFIGURATION,
                                     bootstrap_servers=['localhost:9092'],
                                     value_deserializer=lambda m: json.loads(m.decode('ascii')))
            self.__coordinating_monitors(consumer)
            # app_logger.get.info("Topic is empty or connection is lost. Reconnecting...")
            print("Topic is empty or connection is lost. Reconnecting...")


class Monitor:
    """The root (abstract) Class for the Monitor Tools
        accept      --  Accept the updater to perform the DB operations
        monitoring  --  performing the monitoring job for memory, CPU and Disk
    """

    def __init__(self):
        pass

    @staticmethod
    def start_monitor_system():
        # start config monitor
        ConfigMgr().start()
        # start coordinator
        MonitorCoordinator().start()


class HANAServerDBOperatorService:
    """ HANA Server DB operator, responsible for all DB relative operations, it's designed as singleton.
    To get the instance of this class: HANAServerDBOperatorService.instance()
    Initialize the class using HANAServerDBOperatorService() will raise an exception.
    """
    __instance = None

    @staticmethod
    def instance():
        """static access method for singleton"""
        if HANAServerDBOperatorService.__instance is None:
            HANAServerDBOperatorService()
        return HANAServerDBOperatorService.__instance

    def __init__(self):
        # implement the singleton class
        if HANAServerDBOperatorService.__instance is not None:
            raise MonitorDBOpError("This class is a singleton, use HANAServerDBOperatorService.instance() instead")
        else:
            HANAServerDBOperatorService.__instance = self
            self.__monitor_dao = HANAMonitorDAO(Mc.get_hana_server(),
                                                Mc.get_hana_port(),
                                                Mc.get_hana_user(),
                                                Mc.get_hana_password())
            self.__logger = Mu.get_logger(Mc.LOGGER_MONITOR_SERVER_DB_OPERATOR)

    def get_db_configuration(self, component, name):
        db_output = self.__monitor_dao.get_configuration(component, name)
        try:
            if name.endswith("_INT"):
                config_value = int(db_output[0][0]) if db_output else None
            else:
                config_value = db_output[0][0] if db_output else None
        except Exception as ex:
            config_value = None
            Mu.log_warning(self.__logger, "Parsing DB output failed in 'get_db_configuration' "
                                          "with error: {0}, the output: {1}".format(ex, db_output))
        return config_value


if __name__ == '__main__':
    Monitor.start_monitor_system()

