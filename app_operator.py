from kafka import KafkaConsumer
from util import ActionType
from util import MonitorConst as Mc
from util import MonitorUtility as Mu
from util import KafKaUtility as Ku
from abc import ABC, abstractmethod
from operation.os_operations import LinuxOperator
import time
import threading
import json


class AppOperator(threading.Thread):
    """Context class for operation strategy"""
    def __init__(self):
        super().__init__()
        self.__logger = Mu.get_logger(Mc.LOGGER_MONITOR_OPERATOR_APP)
        # currently, only support shutdown and log backup clean
        self.switcher = {
            ActionType.SHUTDOWN.value: AppOperator.__HANACloser(self.__logger),
            ActionType.CLEAN_LOG_BACKUP.value: AppOperator.__HANALogCleaner(self.__logger)
        }
        self.__app_operation_interval = 5  # TODO config in DB or ini ? or just hard code in util?

    def __operate(self, consumer):
        """ poll from consumer, performing the related operation"""

        # for msg in consumer:
        #     # {action: {Mc.FIELD_SERVER_FULL_NAME: server_name, Mc.FIELD_SID: sid, Mc.FIELD_USER_NAME: user_name}}
        #     for action, info in msg.value:
        #         if action in switcher:
        #             Mu.log_info(self.__logger, "Trying to perform action: {0}...")
        #             switcher[action].operate(info)
        #             Mu.log_info(self.__logger, "Action: {0} is done.")

        app_opp_msg_pack = consumer.poll(update_offsets=True)
        if app_opp_msg_pack:
            for tp, messages in app_opp_msg_pack.items():
                # {action: {Mc.FIELD_SERVER_FULL_NAME: server_name, Mc.FIELD_SID: sid, Mc.FIELD_USER_NAME: user_name}}
                for msg in messages:
                    for action, info in msg.value.items():
                        action_type = int(action)
                        if action_type in self.switcher:
                            Mu.log_info(self.__logger, "Trying to perform action: {0}...".format(action))
                            try:
                                Mu.log_debug(self.__logger, "Action detail: {0}".format(info))
                                self.switcher[action_type].operate(info)
                            except Exception as ex:
                                Mu.log_warning_exc(
                                    self.__logger,
                                    "Perform action failed with {0}, action detail is {1}".format(ex, info))

                            Mu.log_info(self.__logger, "Action: {0} is done.".format(action))

    def run(self):
        """run the thread"""
        consumer = KafkaConsumer(  # Mc.TOPIC_APP_OPERATION,
            group_id=Mc.MONITOR_GROUP_ID,
            bootstrap_servers=["{0}:{1}".format(Mc.get_kafka_server(), Mc.get_kafka_port())],
            value_deserializer=lambda m: json.loads(m.decode('ascii')))

        # assign the topic and seek to end
        Ku.assign_and_seek_to_end(consumer, Mc.TOPIC_APP_OPERATION, Mc.TOPIC_APP_OPERATION)
        Mu.log_info(self.__logger, "Start monitoring app queue...")
        while True:
            try:
                self.__operate(consumer)
            except Exception as ex:
                Mu.log_warning_exc(self.__logger, "Error happened when performing the operation, err: {0}".format(ex))

            time.sleep(self.__app_operation_interval)

    class HANAOperator(ABC):
        def __init__(self, logger):
            super().__init__()
            self._logger = logger
            self._os_operator = LinuxOperator()

        @abstractmethod
        def operate(self, parameter):
            """abstract method, needs to be overwritten in child classes"""
            pass

    class __HANACloser(HANAOperator):
        def __init__(self, logger):
            super().__init__(logger)

        def operate(self, parameter):
            server = parameter[Mc.FIELD_SERVER_FULL_NAME]
            user = parameter[Mc.FIELD_USER_NAME]
            with Mu.open_ssh_connection(self._logger,
                                        self._os_operator,
                                        server,
                                        user,
                                        Mc.get_ssh_default_password()) as ssh:
                if ssh is None:
                    # TODO: notify alarm operator because of the non-standard password
                    Mu.log_debug(self._logger, "Notifying alarm... user: {0}, server: {1}".format(user, server))
                else:
                    Mu.log_debug(self._logger, "Trying shutdown HANA on {0} for user {1}".format(server, user))
                    self._os_operator.shutdown_hana(ssh)

    class __HANALogCleaner(HANAOperator):
        def __init__(self, logger):
            super().__init__(logger)

        def operate(self, parameter):
            server = parameter[Mc.FIELD_SERVER_FULL_NAME]
            user = parameter[Mc.FIELD_USER_NAME]
            sid = parameter[Mc.FIELD_SID]
            with Mu.open_ssh_connection(self._logger,
                                        self._os_operator,
                                        server,
                                        user,
                                        Mc.get_ssh_default_password()) as ssh:
                if ssh is None:
                    # TODO: notify alarm operator because of the non-standard password
                    Mu.log_debug(self._logger, "Notifying alarm... user: {0}, server: {1}".format(user, server))
                else:
                    Mu.log_debug(self._logger, "Trying clean log backup on {0} for user {1}".format(server, user))
                    self._os_operator.clean_log_backup(ssh, sid)


if __name__ == '__main__':
    AppOperator().start()
