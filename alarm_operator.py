import json

from threading import Thread
from kafka import KafkaConsumer
from kafka import KafkaProducer
from util import MonitorUtility as Mu
from util import MonitorConst as Mc
from util import InfoType
from util import Email
from util import ActionType
from datetime import datetime
from operation.db_operations import HANAMonitorDAO


class AlarmOperator(Thread):

    def __init__(self):
        super().__init__()

        self.__db_operator = AlarmOperator.__HANAOperatorService()
        self.__logger = Mu.get_logger(Mc.LOGGER_MONITOR_OPERATOR_ALARM)
        self.cpu_threshold = 0
        self.mem_threshold = 0
        self.disk_threshold = 0
        self.email_sender = ""
        self.operation_time = ""
        self.max_failure_times = 3
        self.mem_emergency_threshold = 0
        self.__producer = KafkaProducer(
            bootstrap_servers=["{0}:{1}".format(Mc.get_kafka_server(), Mc.get_kafka_port())],
            value_serializer=lambda v: json.dumps(v).encode("ascii"))
        self.__topic = Mc.TOPIC_APP_OPERATION

    def __operate(self, consumer):
        operators = {
            InfoType.MEMORY.value: self.__operate_memory,
            InfoType.CPU.value: self.__operate_cpu,
            InfoType.DISK.value: self.__operate_disk
        }
        for msg in consumer:
            if not msg or not msg.value:
                continue

            # update configuration
            self.__update_configuration(msg.value)

            # process filtered message
            alarm = {}

            if Mc.MSG_TYPE in msg.value and msg.value[Mc.MSG_TYPE] in operators:
                top5_consumers = operators[msg.value[Mc.MSG_TYPE]](msg.value)
                server_id = msg.value[Mc.FIELD_SERVER_ID]
                msg_type = msg.value[Mc.MSG_TYPE]
                if top5_consumers:
                    server_name = top5_consumers[Mc.FIELD_SERVER_FULL_NAME]
                    mem_free = top5_consumers[Mc.INFO_FREE]
                    mem_total = top5_consumers[Mc.INFO_TOTAL]

                    highest_consumer = AlarmOperator.__get_highest_consumption_info(top5_consumers[Mc.INFO_USAGE])
                    email = highest_consumer[Mc.FIELD_EMAIL]
                    employee_name = highest_consumer[Mc.FIELD_EMPLOYEE_NAME]
                    user_name = highest_consumer[Mc.FIELD_USER_NAME]  # for shutdown (in app operator)
                    sid = highest_consumer[Mc.FIELD_SID]
                    mem_usage = highest_consumer[Mc.FIELD_USAGE]

                    if msg_type == InfoType.MEMORY.value and mem_free / mem_total <= self.mem_emergency_threshold:
                        # trigger the emergency shutdown
                        admin = self.__db_operator.get_email_admin(server_id)

                        Mu.log_info(self.__logger,
                                    "Try to sending emergency shutdown email for {0} on {1}, because server "
                                    "is running out of memory and {2} is consuming highest "
                                    "({3}%) memory.".format(sid, server_name, user_name, mem_usage))
                        # sending email to the owner of the instance
                        Email.send_emergency_shutdown_email(
                            self.email_sender, email, sid, server_name, employee_name, admin, mem_usage, InfoType.MEMORY
                        )

                        self.__send_shutdown_message(server_name, sid, user_name)
                        continue

                    # If it's not working time, skip sending email and shutdown )
                    if not Mu.is_current_time_working_time(self.operation_time):
                        Mu.log_info(self.__logger, "Skip alarm operations because of the non-working time.")
                        continue

                    email_flag = 0
                    # update alarm info
                    if server_id in alarm:
                        if msg_type not in alarm[server_id]:
                            alarm[server_id][msg_type] = {Mc.INFO_ALARM_TIME: datetime.now(), Mc.INFO_ALARM_NUM: 0}
                    else:
                        alarm[server_id] = {msg_type: {Mc.INFO_ALARM_TIME: datetime.now(), Mc.INFO_ALARM_NUM: 0}}

                    if alarm[server_id][msg_type][Mc.INFO_ALARM_NUM] == 0:
                        alarm[server_id][msg_type][Mc.INFO_ALARM_NUM] = 1
                        alarm[server_id][msg_type][Mc.INFO_ALARM_TIME] = datetime.now()
                        # send email
                        email_flag = 1
                    else:
                        pre_time = alarm[server_id][msg_type][Mc.INFO_ALARM_TIME]
                        cur_time = datetime.now()
                        if (cur_time - pre_time).total_seconds() >= 3600:  # every hour sending next alarm mail
                            alarm[server_id][msg_type][Mc.INFO_ALARM_NUM] += 1
                            alarm[server_id][msg_type][Mc.INFO_ALARM_TIME] = cur_time
                            if alarm[server_id][msg_type][Mc.INFO_ALARM_NUM] > self.max_failure_times and \
                                    msg_type == InfoType.MEMORY.value:
                                email_flag = 2
                            else:
                                email_flag = 1

                    if email_flag >= 1:
                        # sending email
                        email_to = [c[Mc.FIELD_EMAIL] for c in top5_consumers[Mc.INFO_USAGE] if c[Mc.FIELD_EMAIL]]
                        admin = self.__db_operator.get_email_admin(server_id)
                        Mu.log_debug(self.__logger, "[MEM] Sending email to:{0}".format(email_to))

                        Email.send_warning_email(self.email_sender,
                                                 email_to,
                                                 top5_consumers[Mc.MSG_TYPE],
                                                 server_name,
                                                 top5_consumers,
                                                 admin)

                    if email_flag == 2:
                        # trigger the shutdown
                        admin = self.__db_operator.get_email_admin(server_id)

                        # sending email to the owner of the instance
                        Mu.log_info(self.__logger,
                                    "Try to sending shutdown email for {0} on {1}, because server "
                                    "is running out of memory and {2} is consuming highest "
                                    "({3}%) memory.".format(sid, server_name, user_name, mem_usage))
                        Email.send_shutdown_email(
                            self.email_sender, email, sid, server_name, employee_name, admin, mem_usage, InfoType.MEMORY
                        )

                        # send shutdown message
                        self.__send_shutdown_message(server_name, sid, user_name)

                else:
                    # everything is good, reset the alarm for server_id and msg type
                    if server_id in alarm and msg_type in alarm[server_id]:
                        alarm[server_id][msg_type] = {}

    def __update_configuration(self, msg):
        """update configuration from message"""
        # belows only are used here, so do not create consts in util
        cpu_threshold_key = "THRESHOLD_CPU_USAGE_WARN_INT"
        mem_threshold_key = "THRESHOLD_MEM_USAGE_WARN_INT"
        disk_threshold_key = "THRESHOLD_DISK_USAGE_WARN_INT"
        email_sender_key = "EMAIL_SENDER"
        operation_time_key = "OPERATION_TIME"
        max_failure_times_key = "MAX_FAILURE_TIMES_INT"
        mem_emergency_threshold_key = "THRESHOLD_MEM_EMERGENCY_SHUTDOWN_INT"

        # update configuration
        if cpu_threshold_key in msg and msg[cpu_threshold_key] != self.cpu_threshold:
            self.cpu_threshold = msg[cpu_threshold_key]
            Mu.log_info(self.__logger, "CPU usage threshold is: {0}".format(self.cpu_threshold))

        if mem_threshold_key in msg and msg[mem_threshold_key] != self.mem_threshold:
            self.mem_threshold = msg[mem_threshold_key]
            Mu.log_info(self.__logger, "Memory usage threshold is: {0}".format(self.mem_threshold))

        if disk_threshold_key in msg and msg[disk_threshold_key] != self.disk_threshold:
            self.mem_threshold = msg[disk_threshold_key]
            Mu.log_info(self.__logger, "Disk usage threshold is: {0}".format(self.disk_threshold))

        if email_sender_key in msg and msg[email_sender_key] != self.email_sender:
            self.email_sender = msg[email_sender_key]
            Mu.log_info(self.__logger, "Email sender is: {0}".format(self.email_sender))

        if operation_time_key in msg and msg[operation_time_key] != self.operation_time:
            self.operation_time = msg[operation_time_key]
            Mu.log_info(self.__logger, "Operation time is: {0}".format(self.operation_time))

        if max_failure_times_key in msg and msg[max_failure_times_key] != self.max_failure_times:
            self.max_failure_times = msg[max_failure_times_key]
            Mu.log_info(self.__logger, "Max failure times is: {0}".format(self.max_failure_times))

        if mem_emergency_threshold_key in msg and msg[mem_emergency_threshold_key] != self.mem_emergency_threshold:
            self.mem_emergency_threshold = msg[mem_emergency_threshold_key]
            Mu.log_info(self.__logger, "Memory emergency threshold is: {0}".format(self.mem_emergency_threshold))

    def __send_shutdown_message(self, server_name, sid, user_name):
        Mu.log_debug(
            self.__logger, "Sending emergency shutdown message {0} on {1} ...".format(sid, server_name))
        # send shutdown message
        self.__producer.send(self.__topic, AlarmOperator.__generate_action_message(
            server_name, sid, user_name, ActionType.SHUTDOWN.value))

        Mu.log_debug(
            self.__logger, "Emergency shutdown message {0} on {1} is sent".format(sid, server_name))

    def __operate_memory(self, info):
        mem_free = info[Mc.FIELD_MEM_FREE]
        mem_total = info[Mc.FIELD_MEM_TOTAL]
        server_id = info[Mc.FIELD_SERVER_ID]
        check_id = info[Mc.FIELD_CHECK_ID]

        if mem_free is None or mem_free < 0 or mem_total is None or mem_total <= 0 or self.mem_threshold <= 0:
            return

        free_mem_threshold = ((100 - self.mem_threshold) * mem_total) / 100
        Mu.log_info(self.__logger, "Server:{0}, check_id:{1}, free Memory:{2}, threshold:{3}".format(
            server_id, check_id, mem_free, free_mem_threshold))

        # prepare all info if size of free memory < threshold
        if mem_free < free_mem_threshold:
            mem_consumers = list(info[Mc.MSG_INFO].items())

            mem_consumers.sort(key=lambda v: v[1], reverse=True)  # sort by desc
            del mem_consumers[5:]  # only keep the top 5

            users = [user[0] for user in mem_consumers]
            top_5_consumers = self.__get_users_info(server_id, check_id, users, InfoType.MEMORY, mem_free, mem_total)

            # combine usage info
            for user_info in top_5_consumers.get(Mc.INFO_USAGE, []):
                # {"user1": 12.2, "user2": 13.2}
                user_info[Mc.FIELD_USAGE] = info[Mc.MSG_INFO][user_info[Mc.FIELD_USER_NAME]]  # set the usage

            return top_5_consumers

    def __operate_cpu(self, info):
        cpu_usage = info[Mc.FIELD_CPU_UTILIZATION]
        server_id = info[Mc.FIELD_SERVER_ID]
        check_id = info[Mc.FIELD_CHECK_ID]

        if cpu_usage is None or cpu_usage < 0 or self.cpu_threshold <= 0:
            return

        Mu.log_debug(self.__logger, "Server:{0}, check_id:{1}, cpu usage:{2}, threshold:{3}".format(
            server_id, check_id, cpu_usage, self.cpu_threshold))

        # prepare all info if size of free memory < threshold
        if cpu_usage > self.cpu_threshold:
            cpu_consumers = list(info[Mc.MSG_INFO].items())
            cpu_consumers.sort(key=lambda v: v[1], reverse=True)  # sort by desc
            del cpu_consumers[5:]  # only keep the top 5

            users = [user[0] for user in cpu_consumers]
            top_5_consumers = self.__get_users_info(server_id, check_id, users, InfoType.CPU, 100 - cpu_usage, -1)

            # combine usage info
            for user_info in top_5_consumers.get(Mc.INFO_USAGE, []):
                # {"user1": 12.2, "user2": 13.2}
                user_info[Mc.FIELD_USAGE] = info[Mc.MSG_INFO][user_info[Mc.FIELD_USER_NAME]]  # set the usage

            return top_5_consumers

    def __operate_disk(self, info):
        server_id = info[Mc.FIELD_SERVER_ID]
        check_id = info[Mc.FIELD_CHECK_ID]

        disk_free = info[Mc.FIELD_DISK_FREE]
        disk_total = info[Mc.FIELD_DISK_TOTAL]

        free_disk_threshold = ((100 - self.disk_threshold) * disk_total) / 100

        Mu.log_debug(self.__logger, "Server:{0}, check_id:{1}, free disk:{2}, threshold:{3}".format(
            server_id, check_id, disk_free, free_disk_threshold))

        if disk_free is None or disk_free < 0 or disk_total is None or disk_total <= 0 or self.disk_threshold <= 0:
            return

        # prepare all info if size of free memory < threshold
        if disk_free < free_disk_threshold:
            # {"folder": {"user1":3245}, "folder2":{"user2":222}, "folder3":{"user3":99999}}
            disk_consumers = list(info[Mc.MSG_INFO].items())
            disk_consumers.sort(key=lambda v: next(iter(v[1].values())), reverse=True)  # sort by desc
            del disk_consumers[5:]  # only keep the top 5

            users = []
            for folder in disk_consumers:
                users.append(next(iter(folder[1].keys())))

            top_5_consumers = self.__get_users_info(server_id, check_id, users, InfoType.DISK, disk_free, disk_total)

            # combine usage info
            folders_info = []
            for folder in disk_consumers:
                folder_info = {
                    Mc.FIELD_FOLDER: folder[0],
                    Mc.FIELD_USER_NAME: next(iter(folder[1].keys())),
                    Mc.FIELD_USAGE: next(iter(folder[1].values()))
                }
                for user_info in top_5_consumers.get(Mc.INFO_USAGE, []):
                    if folder_info[Mc.FIELD_USER_NAME] == user_info[Mc.FIELD_USER_NAME]:
                        folder_info.update(user_info)
                folders_info.append(folder_info)

            top_5_consumers[Mc.INFO_USAGE] = folders_info
            return top_5_consumers

    def __get_users_info(self, server_id, check_id, consumers, info_type, free, total):
        users_info = self.__db_operator.get_users_info(server_id, consumers)

        if not users_info or len(users_info) == 0 or Mc.FIELD_SERVER_FULL_NAME not in users_info[0]:
            Mu.log_warning(self.__logger, "Cannot get any users info for the server '{0}' type {1} of {2}".format(
                               server_id, info_type, consumers))
            return []

        # even no user matched, server full name will still be returned
        server_full_name = users_info[0][Mc.FIELD_SERVER_FULL_NAME]

        if len(users_info) >= 1:  # maybe can't find any users in the system
            top_5_consumers = {
                Mc.MSG_TYPE: info_type,
                Mc.FIELD_SERVER_ID: server_id,
                Mc.FIELD_SERVER_FULL_NAME: server_full_name,
                Mc.FIELD_CHECK_ID: check_id,
                Mc.INFO_TOTAL: total,
                Mc.INFO_FREE: free,
                Mc.INFO_USAGE: users_info
            }

            return top_5_consumers

    @staticmethod
    def __generate_action_message(server_name, sid, user_name, action):
        return {action: {Mc.FIELD_SERVER_FULL_NAME: server_name, Mc.FIELD_SID: sid, Mc.FIELD_USER_NAME: user_name}}

    @staticmethod
    def __get_highest_consumption_info(top5_consumers):
        usage = -1
        highest_consumer = {}
        for consumer in top5_consumers:
            if consumer[Mc.FIELD_USAGE] > usage and not consumer[Mc.FIELD_FILTER_FLAG]:  # skip filtered instance
                usage = consumer[Mc.FIELD_USAGE]
                highest_consumer = consumer
        return highest_consumer

    def run(self):
        """run the thread"""
        while True:
            consumer = KafkaConsumer(
                topics=[Mc.TOPIC_SERVER_MONITORING_FILTERED_INFO, Mc.TOPIC_CONFIGURATION],
                group_id=Mc.MONITOR_GROUP_ID_ALARM,  # should be in different group with others
                bootstrap_servers=["{0}:{1}".format(Mc.get_kafka_server(), Mc.get_kafka_port())],
                value_deserializer=lambda m: json.loads(m.decode('ascii')))
            self.__operate(consumer)
            Mu.log_warning(self.__logger, "Topic is empty or connection is lost. Trying to reconnect...")

    class __HANAOperatorService:
        """ Internal HANA Server DB operator, responsible for all DB relative operations """

        def __init__(self):
            # implement the singleton class
            self.__monitor_dao = HANAMonitorDAO(
                Mc.get_hana_server(), Mc.get_hana_port(), Mc.get_hana_user(), Mc.get_hana_password())
            self.__logger = Mu.get_logger(Mc.LOGGER_MONITOR_OPERATOR_ALARM)

        def get_email_admin(self, server_id):
            return self.__monitor_dao.get_email_admin(server_id)

        def get_users_info(self, server_id, users):
            consumers = []
            for user in users:
                consumers.append(user[0])

            db_output = self.__monitor_dao.get_info_by_5_sidadm_users(server_id, consumers)
            try:
                users_info = [{
                    Mc.FIELD_SERVER_FULL_NAME: u[1],
                    Mc.FIELD_SID: u[2],
                    Mc.FIELD_USER_NAME: u[3],
                    Mc.FIELD_FILTER_FLAG: u[4],
                    Mc.FIELD_EMPLOYEE_NAME: u[5],
                    Mc.FIELD_EMAIL: u[6]} for u in db_output]
            except Exception as ex:
                Mu.log_warning(self.__logger,
                               "Parsing DB output from 'get_info_by_5_sidadm_users' failed with error: {0}, "
                               "the output: {1}".format(ex, db_output))
                users_info = []

            return users_info


if __name__ == '__main__':
    AlarmOperator().start()
