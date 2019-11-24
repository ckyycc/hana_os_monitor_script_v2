import json

from errors import MonitorDBOpError

import threading
import time

from operation.os_operations import LinuxMonitorDAO
from util import MonitorUtility as Mu
from util import MonitorConst as Mc
from operation.db_operations import HANAMonitorDAO

from kafka import KafkaProducer
from kafka import KafkaConsumer

from kafka.errors import KafkaError


class MonitorCoordinator(threading.Thread):
    """
    Coordinator for all the agents, it responsible for starting/stopping/restarting all the the agents
    """
    def __init__(self):
        super().__init__()
        self.__logger = Mu.get_logger(Mc.LOGGER_MONITOR_COORDINATOR)
        self.__configs = {}

    def __coordinating_monitors(self, consumer):
        """
        Coordinating (start/stop/restart) all the agents
        :param consumer: kafka consumer
        """
        Mu.log_debug(self.__logger, "Coordinator is listening on topic for configurations.")
        for msg in consumer:
            Mu.log_debug(self.__logger, "New configs are coming...")
            if self.__update_configs(msg.value):
                # start/restart all agents, current design is restart all agents if any config is changed
                servers = self.__configs.get(Mc.DB_CONFIGURATION_SERVER, [])
                for server in servers:
                    self.__restart_agent(server[Mc.FIELD_SERVER_FULL_NAME],
                                         server[Mc.FIELD_MOUNT_POINT],
                                         server[Mc.FIELD_OS])

    def __restart_agent(self, server, mounting_point, os):
        ssh = self.__open_ssh_connection(server,
                                         Mc.get_ssh_default_user(),
                                         Mu.get_decrypt_string(Mc.get_rsa_key_file(), Mc.get_ssh_default_password()))

        if ssh is not None:
            Mu.log_debug(self.__logger, "Restarting {0}".format(server))
            LinuxMonitorDAO().restart_agent(ssh)

    def __update_configs(self, config):
        """
        Update local configs with the provided config
        :param config: config received from kafka topic
        :return: true -> config updated; false -> config is not changed
        """
        flag = False

        if not config:
            return flag

        for key, value in config.items():
            if value != self.__configs.get(key, None):
                self.__configs[key] = value
                flag = True
        return flag

    def __open_ssh_connection(self, server_name, user_name, user_password):
        Mu.log_debug(self.__logger, "Trying to connect {0}.".format(server_name))
        ssh = LinuxMonitorDAO().open_ssh_connection(server_name, user_name, user_password)
        if ssh is not None:
            Mu.log_debug(self.__logger, "Connected {0}.".format(server_name))
        return ssh

    def __close_ssh_connection(self, ssh):
        LinuxMonitorDAO().close_ssh_connection(ssh)

    def run(self):
        """run the thread"""
        while True:
            consumer = KafkaConsumer(Mc.TOPIC_CONFIGURATION,
                                     group_id=Mc.MONITOR_GROUP_ID,
                                     bootstrap_servers=['localhost:9092'],
                                     value_deserializer=lambda m: json.loads(m.decode('ascii')))
            self.__coordinating_monitors(consumer)
            Mu.log_warning(self.__logger, "Topic is empty or connection is lost. Trying to reconnect...")
