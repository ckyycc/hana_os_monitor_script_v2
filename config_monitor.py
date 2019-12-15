import threading

from operation.db_operations import HANAMonitorDAO
from util import MonitorUtility as Mu
from util import MonitorConst as Mc
from util import KafKaUtility as Ku
import time


class ConfigMonitor(threading.Thread):
    """Configuration manager pushes the all the configurations from to DB to the kafka topic when it starts.
      And then, it keeps monitoring the configurations from DB, if there is anything changes, it will push the changes
      to kafka topic
    """
    def __init__(self):
        super().__init__()
        self.__producer = Ku.get_producer()
        self.__logger = Mu.get_logger(Mc.LOGGER_MONITOR_CONFIG_MGR)
        self.__topic = Mc.TOPIC_CONFIGURATION
        self.__configs = {}

    def __get_configurations(self, operator):
        # get all configurations from db
        db_output_configs = operator.get_configuration()
        try:
            configs = {cfg[0]: int(cfg[1]) if cfg[0].endswith("_INT") else cfg[1] for cfg in db_output_configs}
        except Exception as ex:
            Mu.log_warning(self.__logger,
                           "Parsing DB output from 'get_configuration' failed with error: {0}, the output: {1}"
                           .format(ex, db_output_configs))
            configs = {}

        # get all servers info from db
        db_output_servers = operator.get_server_full_names()
        try:
            configs[Mc.DB_CONFIGURATION_SERVER] = [{Mc.FIELD_SERVER_ID: server[0],
                                                    Mc.FIELD_SERVER_FULL_NAME: server[1],
                                                    Mc.FIELD_MOUNT_POINT: server[2],
                                                    Mc.FIELD_OS: server[3]} for server in db_output_servers]

        except Exception as ex:
            Mu.log_warning(self.__logger,
                           "Parsing DB output failed in 'get_server_full_names' with error: {0}, the output: {1}"
                           .format(ex, db_output_servers))
            configs[Mc.DB_CONFIGURATION_SERVER] = []

        return configs

    def __monitoring_configurations(self, operator):
        updated_configs = {}

        for key, value in self.__get_configurations(operator).items():
            if value != self.__configs.get(key, None):
                updated_configs[key] = value
                self.__configs[key] = value

        if updated_configs:
            Mu.log_debug(self.__logger, "Sending updated configs {0} to queue...".format(updated_configs))
            self.__producer.send(self.__topic, updated_configs)
            # block until all async messages are sent
            self.__producer.flush()
            Mu.log_debug(self.__logger, "Sent updated configs to queue.")
        else:
            Mu.log_debug(self.__logger, "No update for configurations ...")

    def run(self):
        """run the thread"""
        operator = HANAMonitorDAO(Mc.get_hana_server(), Mc.get_hana_port(), Mc.get_hana_user(), Mc.get_hana_password())
        while True:
            try:
                self.__monitoring_configurations(operator)
            except Exception as ex:
                Mu.log_warning_exc(self.__logger, "Error occurred when monitoring configuration, Error: {0}".format(ex))

            time.sleep(self.__configs.get("CHECK_INTERVAL_CONFIG_INT", 300))


if __name__ == '__main__':
    ConfigMonitor().start()
