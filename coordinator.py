import threading
import time
from datetime import datetime
from operation.os_operations import LinuxOperator
from util import MonitorUtility as Mu
from util import MonitorConst as Mc
from util import KafKaUtility as Ku
from util import InfoType


class MonitorCoordinator(threading.Thread):
    """
    Coordinator for all the agents, it responsible for starting/stopping/restarting all the the agents
    """
    def __init__(self):
        super().__init__()
        self.__logger = Mu.get_logger(Mc.LOGGER_MONITOR_COORDINATOR)
        self.__configs = {}
        self.__os_operator = LinuxOperator()
        self.__heartbeat_flag = False
        self.__heartbeat_interval = Mc.get_heartbeat_check_interval()
        self.__heartbeat_timeout = Mc.get_heartbeat_timeout()
        self.__heartbeat_restart_agent_interval = Mc.get_heartbeat_operation_interval()
        self.__heartbeat_agent_restart_info = {}

    def __coordinating_monitors(self, consumer):
        """
        Coordinating (start/stop/restart) all the agents
        :param consumer: kafka consumer
        """
        Mu.log_debug(self.__logger, "Coordinator is listening on topic for configurations.")
        for msg in consumer:
            try:
                Mu.log_debug(self.__logger, "New configs are coming...")
                if self.__update_configs(msg.value):
                    # start/restart all agents, current design is restart all agents if any config is changed
                    servers = self.__configs.get(Mc.DB_CONFIGURATION_SERVER, [])
                    for server in servers:
                        self.__restart_agent(server[Mc.FIELD_SERVER_FULL_NAME],
                                             server[Mc.FIELD_SERVER_ID],
                                             server[Mc.FIELD_MOUNT_POINT],
                                             Mc.get_agent_path(),
                                             self.__configs.get("CHECK_INTERVAL_MEM_INT", 60),
                                             self.__configs.get("CHECK_INTERVAL_CPU_INT", 300),
                                             self.__configs.get("CHECK_INTERVAL_DISK_INT", 3600),
                                             self.__configs.get("CHECK_INTERVAL_INSTANCE_INT", 300))

                if self.__check_configuration() and not self.__heartbeat_flag:
                    self.__heartbeat_flag = True
                    # start heart beat thread
                    heartbeat_thread = threading.Thread(target=self.__process_heartbeat)
                    heartbeat_thread.start()
            except Exception as ex:
                Mu.log_warning_exc(self.__logger, "Error occurred when coordinating the monitors, Err: {0}".format(ex))

    def __process_heartbeat(self):
        # Mc.TOPIC_AGENT_HEARTBEAT,  seek_to_end failed with no partition assigned, try manually assign
        consumer = Ku.get_consumer(Mc.MONITOR_GROUP_ID_COORDINATOR)
        # skip all previous messages, not care about past
        # consumer.assign([TopicPartition(topic=Mc.TOPIC_AGENT_HEARTBEAT, partition=0)])
        # use assign instead subscribe because the error: https://github.com/dpkp/kafka-python/issues/601
        Ku.assign_and_seek_to_end(consumer, Mc.TOPIC_AGENT_HEARTBEAT, Mc.TOPIC_AGENT_HEARTBEAT)
        # consumer.assign(Ku.get_assignments(consumer, [Mc.TOPIC_AGENT_HEARTBEAT]))
        # consumer.seek_to_end(*Ku.get_topic_partitions(consumer, Mc.TOPIC_AGENT_HEARTBEAT))
        # consumer.seek_to_end()

        # init heartbeat_info for all servers
        heartbeat_info = {s[Mc.FIELD_SERVER_ID]: {
            InfoType.MEMORY.value: datetime.now()} for s in self.__configs.get(Mc.DB_CONFIGURATION_SERVER, [])}

        while True:
            try:
                Mu.process_heartbeat(self.__logger,
                                     heartbeat_info,
                                     consumer,
                                     self.__heartbeat_timeout,
                                     self.__restart_agent_via_server_id)
            except Exception as ex:
                Mu.log_warning_exc(self.__logger, "Error occurred when checking heartbeat, error: {0}".format(ex))
            time.sleep(self.__heartbeat_interval)

    def __restart_agent_via_server_id(self, server_id):
        pre_time = self.__heartbeat_agent_restart_info.get(server_id, datetime.min)
        cur_time = datetime.now()

        if (cur_time - pre_time).total_seconds() >= self.__heartbeat_restart_agent_interval:
            servers = [s for s in self.__configs.get(Mc.DB_CONFIGURATION_SERVER, [])
                       if s[Mc.FIELD_SERVER_ID] == server_id]
            for server in servers:
                Mu.log_info(self.__logger, "Restarting agent on {0}.".format(server[Mc.FIELD_SERVER_FULL_NAME]))
                self.__restart_agent(server[Mc.FIELD_SERVER_FULL_NAME],
                                     server[Mc.FIELD_SERVER_ID],
                                     server[Mc.FIELD_MOUNT_POINT],
                                     Mc.get_agent_path(),
                                     self.__configs.get("CHECK_INTERVAL_MEM_INT", 60),
                                     self.__configs.get("CHECK_INTERVAL_CPU_INT", 300),
                                     self.__configs.get("CHECK_INTERVAL_DISK_INT", 3600),
                                     self.__configs.get("CHECK_INTERVAL_INSTANCE_INT", 300))
                Mu.log_info(self.__logger,
                            "Restarting agent on {0} is finished.".format(server[Mc.FIELD_SERVER_FULL_NAME]))
        else:
            Mu.log_debug(self.__logger, ("Heartbeat failed for {0}, but did not try to restart agent due to the "
                                         "configured email sending interval time.").format(server_id))

    def __restart_agent(self, server, server_id, mount_point, agent_path,
                        mem_interval, cpu_interval, disk_interval, instance_interval):

        with Mu.open_ssh_connection(self.__logger,
                                    self.__os_operator,
                                    server,
                                    Mc.get_ssh_default_user(),
                                    Mc.get_ssh_default_password()) as ssh:
            Mu.log_debug(self.__logger, "Restarting {0}".format(server))
            self.__os_operator.restart_agent(
                ssh, server_id, mount_point, agent_path, mem_interval, cpu_interval, disk_interval, instance_interval)
            Mu.log_debug(self.__logger, "Restarting of {0} is done".format(server))

    def __check_configuration(self):
        servers = self.__configs.get(Mc.DB_CONFIGURATION_SERVER, [])
        interval_mem = self.__configs.get("CHECK_INTERVAL_MEM_INT", 0)
        interval_cpu = self.__configs.get("CHECK_INTERVAL_CPU_INT", 0)
        interval_disk = self.__configs.get("CHECK_INTERVAL_DISK_INT", 0)
        interval_instance = self.__configs.get("CHECK_INTERVAL_INSTANCE_INT", 0)

        return servers and interval_mem and interval_cpu and interval_disk and interval_instance

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

    def run(self):
        """run the thread"""
        while True:
            consumer = Ku.get_consumer(Mc.MONITOR_GROUP_ID_COORDINATOR, Mc.TOPIC_CONFIGURATION)
            self.__coordinating_monitors(consumer)
            Mu.log_warning(self.__logger, "Topic is empty or connection is lost. Trying to reconnect...")


if __name__ == '__main__':
    MonitorCoordinator().start()
