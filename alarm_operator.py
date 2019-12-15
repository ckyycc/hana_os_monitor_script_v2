from util import MonitorUtility as Mu
from util import MonitorConst as Mc
from util import KafKaUtility as Ku
from util import InfoType
from util import Email
from util import ActionType
from datetime import datetime
from operation.db_operations import HANAMonitorDAO
import time
import threading


class AlarmOperator(threading.Thread):

    def __init__(self):
        super().__init__()

        self.__db_operator = AlarmOperator.__HANAOperatorService()
        self.__logger = Mu.get_logger(Mc.LOGGER_MONITOR_OPERATOR_ALARM)
        self.__heartbeat_interval = Mc.get_heartbeat_check_interval()
        self.__heartbeat_timeout = Mc.get_heartbeat_timeout()
        self.__heartbeat_email_interval = Mc.get_heartbeat_operation_interval()
        self.cpu_threshold = 0
        self.mem_threshold = 0
        self.disk_threshold = 0
        self.email_sender = ""
        self.operation_time = ""
        self.max_failure_times = 3
        self.mem_emergency_threshold = 0
        self.check_interval = 0  # the interval for sending email / performing emergency shutdown
        self.servers = []

        self.__producer = Ku.get_producer()
        self.__topic = Mc.TOPIC_APP_OPERATION
        self.__heartbeat_email_info = {}

    def __operate(self, consumer):
        operators = {
            InfoType.MEMORY.value: self.__operate_memory,
            InfoType.CPU.value: self.__operate_cpu,
            InfoType.DISK.value: self.__operate_disk
        }
        alarm = {}
        emergency_alarm = {}
        Mu.log_info(self.__logger, "Start processing alarm.")

        for msg in consumer:
            if not msg or not msg.value:
                continue
            try:
                # process filtered message
                if Mc.MSG_TYPE not in msg.value or msg.value[Mc.MSG_TYPE] not in operators:
                    # update configuration
                    self.__update_configuration(msg.value)
                    # if configuration is ready, update subscription
                    if self.__check_configuration() and len(consumer.assignment()) < 2:
                        # start heartbeat checking
                        # use assign instead subscribe because the error:
                        # https://github.com/dpkp/kafka-python/issues/601
                        Ku.assign_and_seek_to_end(
                            consumer, Mc.TOPIC_FILTERED_INFO, *[Mc.TOPIC_FILTERED_INFO, Mc.TOPIC_CONFIGURATION])

                        heartbeat_thread = threading.Thread(target=self.__process_heartbeat)
                        heartbeat_thread.start()
                else:
                    # if configuration is not initialized, all data will be ignored
                    top5_consumers = operators[msg.value[Mc.MSG_TYPE]](msg.value)
                    server_id = msg.value[Mc.FIELD_SERVER_ID]
                    msg_type = msg.value[Mc.MSG_TYPE]
                    if top5_consumers:
                        server_name = top5_consumers[Mc.FIELD_SERVER_FULL_NAME]

                        # calculate emergency status
                        if msg_type == InfoType.MEMORY.value:
                            mem_free = top5_consumers[Mc.INFO_FREE]
                            mem_total = top5_consumers[Mc.INFO_TOTAL]
                            # calculate emergency status
                            if float(mem_free) / mem_total * 100 <= self.mem_emergency_threshold:
                                cur_time = datetime.now()
                                pre_time = emergency_alarm.get(server_id, cur_time)
                                if cur_time != pre_time and (cur_time - pre_time).total_seconds() < self.check_interval:
                                    # only perform emergency shutdown every configured interval
                                    continue
                                emergency_alarm[server_id] = cur_time

                                try:
                                    email, employee_name, user_name, sid, mem_usage = \
                                        AlarmOperator.__get_highest_consumption_info(top5_consumers[Mc.INFO_USAGE])
                                except Exception as ex:
                                    Mu.log_warning(
                                        self.__logger,
                                        "Call __get_highest_consumption_info for {0} failed with exception {1}."
                                            .format(top5_consumers[Mc.INFO_USAGE], ex))
                                    continue

                                # trigger the emergency shutdown
                                admin = self.__db_operator.get_email_admin(server_id)

                                Mu.log_info(self.__logger,
                                            "Try to sending emergency shutdown email for {0} on {1}, because server "
                                            "is running out of memory and {2} is consuming highest "
                                            "({3}%) memory.".format(sid, server_name, user_name, mem_usage))
                                # sending email to the owner of the instance
                                Email.send_emergency_shutdown_email(
                                    self.email_sender, email, sid, server_name,
                                    employee_name, admin, mem_usage, InfoType.MEMORY
                                )

                                self.__send_shutdown_message(server_name, sid, user_name)
                                # no need to check further
                                continue
                            else:
                                # reset the emergency alarm for the server if it is not in emergency status
                                emergency_alarm.pop(server_id, None)

                        # If it's not working time, skip sending email and shutdown )
                        if not Mu.is_current_time_working_time(self.operation_time):
                            Mu.log_info(self.__logger, "Skip alarm operations because of the non-working time.")
                            continue

                        email_flag = 0
                        # update alarm info
                        if server_id not in alarm:
                            alarm[server_id] = {msg_type: {Mc.INFO_ALARM_TIME: datetime.now(), Mc.INFO_ALARM_NUM: 0}}
                        elif msg_type not in alarm[server_id]:
                            alarm[server_id][msg_type] = {Mc.INFO_ALARM_TIME: datetime.now(), Mc.INFO_ALARM_NUM: 0}

                        if alarm[server_id][msg_type][Mc.INFO_ALARM_NUM] == 0:
                            alarm[server_id][msg_type][Mc.INFO_ALARM_NUM] = 1
                            alarm[server_id][msg_type][Mc.INFO_ALARM_TIME] = datetime.now()
                            # send email
                            email_flag = 1
                        else:
                            pre_time = alarm[server_id][msg_type][Mc.INFO_ALARM_TIME]
                            cur_time = datetime.now()
                            # every checking interval sending next alarm mail
                            if (cur_time - pre_time).total_seconds() >= self.check_interval:
                                alarm[server_id][msg_type][Mc.INFO_ALARM_NUM] += 1
                                alarm[server_id][msg_type][Mc.INFO_ALARM_TIME] = cur_time
                                if alarm[server_id][msg_type][Mc.INFO_ALARM_NUM] > self.max_failure_times:
                                    email_flag = 2
                                else:
                                    email_flag = 1
                        if email_flag >= 1:
                            # sending email
                            Mu.log_debug(self.__logger, "Top 5 Consumers of server {1} ({2}): {0}".format(
                                top5_consumers, server_id, msg_type))
                            email_to = [c[Mc.FIELD_EMAIL]
                                        for c in top5_consumers[Mc.INFO_USAGE] if c.get(Mc.FIELD_EMAIL, None)]
                            admin = self.__db_operator.get_email_admin(server_id)
                            Mu.log_debug(self.__logger,
                                         "Server {0}:{1}Sending email to:{2}".format(server_id, msg_type, email_to))

                            Email.send_warning_email(self.email_sender,
                                                     email_to,
                                                     top5_consumers[Mc.MSG_TYPE],
                                                     server_name,
                                                     top5_consumers,
                                                     admin)

                        if email_flag == 2:

                            try:
                                email, employee_name, user_name, sid, usage = AlarmOperator\
                                    .__get_highest_consumption_info(top5_consumers[Mc.INFO_USAGE], msg_type)
                                admin = self.__db_operator.get_email_admin(server_id)
                            except Exception as ex:
                                Mu.log_warning(self.__logger,
                                               "Call __get_highest_consumption_info for {0} failed with exception {1}."
                                               .format(top5_consumers[Mc.INFO_USAGE], ex))
                                continue
                            if msg_type == InfoType.MEMORY.value:
                                # sending email to the owner of the instance
                                Mu.log_info(self.__logger,
                                            "Try to sending shutdown email for {0} on {1}, because server "
                                            "is running out of memory and {2} is consuming highest "
                                            "({3}%) memory.".format(sid, server_name, user_name, usage))
                                Email.send_shutdown_email(
                                    self.email_sender, email, sid, server_name,
                                    employee_name, admin, usage, InfoType.MEMORY
                                )
                                # trigger the shutdown --> send shutdown message
                                self.__send_shutdown_message(server_name, sid, user_name)
                            elif msg_type == InfoType.DISK.value:
                                # sending email to the owner of the instance
                                Mu.log_info(self.__logger,
                                            "Try to sending email for {0} on {1}, because server "
                                            "is running out of Disk and {2} is consuming highest "
                                            "({3}%) disk space.".format(sid, server_name, user_name, usage))
                                Email.send_cleaning_disk_email(
                                    self.email_sender, email, sid, server_name,
                                    employee_name, admin, usage, InfoType.DISK
                                )

                                # trigger the shutdown --> send shutdown message
                                self.__send_cleaning_message(server_name, sid, user_name)

                    else:
                        # everything is good, reset the alarm for server_id and msg type
                        if server_id in alarm and msg_type in alarm[server_id]:
                            alarm[server_id][msg_type][Mc.INFO_ALARM_NUM] = 0
            except Exception as ex:
                Mu.log_warning_exc(self.__logger, "Processing alarm failed with {0}.".format(ex))

    def __check_configuration(self):
        return self.servers and \
               self.cpu_threshold and self.mem_threshold and self.disk_threshold and self.mem_emergency_threshold

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
        check_interval_key = "CHECK_INTERVAL_INT"
        # update configuration
        if cpu_threshold_key in msg and msg[cpu_threshold_key] != self.cpu_threshold:
            self.cpu_threshold = msg[cpu_threshold_key]
            Mu.log_info(self.__logger, "CPU usage threshold is: {0}".format(self.cpu_threshold))

        if mem_threshold_key in msg and msg[mem_threshold_key] != self.mem_threshold:
            self.mem_threshold = msg[mem_threshold_key]
            Mu.log_info(self.__logger, "Memory usage threshold is: {0}".format(self.mem_threshold))

        if disk_threshold_key in msg and msg[disk_threshold_key] != self.disk_threshold:
            self.disk_threshold = msg[disk_threshold_key]
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

        if check_interval_key in msg and msg[check_interval_key] != self.check_interval:
            self.check_interval = msg[check_interval_key]
            Mu.log_info(self.__logger, "Check interval  is: {0}".format(self.check_interval))

        if Mc.DB_CONFIGURATION_SERVER in msg and msg[Mc.DB_CONFIGURATION_SERVER] != self.servers:
            self.servers = msg[Mc.DB_CONFIGURATION_SERVER]

    def __process_heartbeat(self):
        # Mc.TOPIC_AGENT_HEARTBEAT,  seek_to_end failed with no partition assigned, try manually assign
        heartbeat_consumer = Ku.get_consumer(Mc.MONITOR_GROUP_ID_ALARM_HEARTBEAT)

        # skip all previous messages, not care about past
        # use assign instead subscribe because the error: https://github.com/dpkp/kafka-python/issues/601
        # consumer.assign([TopicPartition(topic=Mc.TOPIC_AGENT_HEARTBEAT, partition=0)])
        # consumer.seek_to_end()

        Ku.assign_and_seek_to_end(heartbeat_consumer, Mc.TOPIC_AGENT_HEARTBEAT, Mc.TOPIC_AGENT_HEARTBEAT)
        # consumer.assign(Ku.get_assignments(consumer, [Mc.TOPIC_AGENT_HEARTBEAT]))
        # consumer.seek_to_end(*Ku.get_topic_partitions(consumer, Mc.TOPIC_AGENT_HEARTBEAT))

        # init heartbeat_info for all servers
        heartbeat_info = {s[Mc.FIELD_SERVER_ID]: {InfoType.MEMORY.value: datetime.now()} for s in self.servers}
        Mu.log_info(self.__logger, "Start processing heartbeat.")
        while True:
            try:
                Mu.process_heartbeat(self.__logger,
                                     heartbeat_info,
                                     heartbeat_consumer,
                                     self.__heartbeat_timeout,
                                     self.__send_heartbeat_failure_message)
            except Exception as ex:
                Mu.log_warning_exc(self.__logger, "Error occurred when checking heartbeat, error:{0}".format(ex))
            time.sleep(self.__heartbeat_interval)

    def __send_heartbeat_failure_message(self, server_id):
        pre_time = self.__heartbeat_email_info.get(server_id, datetime.min)
        cur_time = datetime.now()

        if (cur_time - pre_time).total_seconds() >= self.__heartbeat_email_interval:
            servers = [s for s in self.servers if s[Mc.FIELD_SERVER_ID] == server_id]
            admin = self.__db_operator.get_email_admin(server_id)
            for server in servers:
                Mu.log_info(self.__logger,
                            "Sending heartbeat failure message for {0}.".format(server[Mc.FIELD_SERVER_FULL_NAME]))
                Email.send_heartbeat_failure_email(self.email_sender, server[Mc.FIELD_SERVER_FULL_NAME], admin)
            # update the email sending time
            self.__heartbeat_email_info[server_id] = cur_time
        else:
            Mu.log_debug(self.__logger, ("heartbeat failed for {0}, but email is not sent due to the configured"
                                         " email sending interval time.").format(server_id))

    def __send_shutdown_message(self, server_name, sid, user_name):
        Mu.log_debug(
            self.__logger, "Sending shutdown message of {0} on {1} ...".format(sid, server_name))
        # send shutdown message
        self.__producer.send(self.__topic, AlarmOperator.__generate_action_message(
            server_name, sid, user_name, ActionType.SHUTDOWN.value))

        Mu.log_debug(
            self.__logger, "Shutdown message of {0} on {1} is sent".format(sid, server_name))

    def __send_cleaning_message(self, server_name, sid, user_name):
        Mu.log_debug(
            self.__logger,
            "Sending log backup cleaning message of {0} on {1} for {2} ...".format(sid, server_name, user_name))
        # send shutdown message
        self.__producer.send(self.__topic, AlarmOperator.__generate_action_message(
            server_name, sid, user_name, ActionType.CLEAN_LOG_BACKUP.value))

        Mu.log_debug(
            self.__logger,
            "Log backup cleaning message of {0} on {1} for {2} is sent".format(sid, server_name, user_name))

    def __operate_memory(self, info):
        mem_free = info[Mc.FIELD_MEM_FREE]
        mem_total = info[Mc.FIELD_MEM_TOTAL]
        server_id = info[Mc.FIELD_SERVER_ID]
        check_id = info[Mc.FIELD_CHECK_ID]

        if mem_free is None or mem_free < 0 or mem_total is None or mem_total <= 0 or self.mem_threshold <= 0:
            return

        free_mem_threshold = ((100 - self.mem_threshold) * mem_total) / 100.0
        Mu.log_debug(self.__logger, "Server:{0}, check_id:{1}, free Memory:{2}, threshold:{3}".format(
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
        cpu_num = info[Mc.FIELD_CPU_NUMBER]
        server_id = info[Mc.FIELD_SERVER_ID]
        check_id = info[Mc.FIELD_CHECK_ID]

        if cpu_usage is None or cpu_usage < 0 or self.cpu_threshold <= 0:
            return

        Mu.log_debug(self.__logger, "Server:{0}, check_id:{1}, cpu usage:{2}, threshold:{3}".format(
            server_id, check_id, cpu_usage, self.cpu_threshold))

        # prepare all info if size of free memory < threshold
        if cpu_usage >= self.cpu_threshold:
            cpu_consumers = list(info[Mc.MSG_INFO].items())
            cpu_consumers.sort(key=lambda v: v[1], reverse=True)  # sort by desc
            del cpu_consumers[5:]  # only keep the top 5

            users = [user[0] for user in cpu_consumers]
            top_5_consumers = self.__get_users_info(server_id, check_id, users, InfoType.CPU, 100 - cpu_usage, -1)

            # combine usage info
            for user_info in top_5_consumers.get(Mc.INFO_USAGE, []):
                # set the usage {"user1": 12.2, "user2": 13.2}
                user_info[Mc.FIELD_USAGE] = info[Mc.MSG_INFO][user_info[Mc.FIELD_USER_NAME]] / float(cpu_num)

            return top_5_consumers

    def __operate_disk(self, info):
        server_id = info[Mc.FIELD_SERVER_ID]
        check_id = info[Mc.FIELD_CHECK_ID]

        disk_free = info[Mc.FIELD_DISK_FREE]
        disk_total = info[Mc.FIELD_DISK_TOTAL]

        free_disk_threshold = ((100 - self.disk_threshold) * disk_total) / 100.0

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

            users = [next(iter(folder[1].keys())) for folder in disk_consumers]
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
    def __get_highest_consumption_info(top5_consumers, msg_type=InfoType.MEMORY.value):
        usage = -1
        highest_consumer = {}
        for consumer in top5_consumers:
            # should skip filtered instance
            if consumer[Mc.FIELD_USAGE] > usage and (
                    Mc.FIELD_FILTER_FLAG not in consumer or not consumer[Mc.FIELD_FILTER_FLAG]):
                usage = consumer[Mc.FIELD_USAGE]
                highest_consumer = consumer

        email = highest_consumer.get(Mc.FIELD_EMAIL, None)
        employee_name = highest_consumer.get(Mc.FIELD_EMPLOYEE_NAME, None)
        user_name = highest_consumer.get(Mc.FIELD_USER_NAME, None)  # for shutdown (in app operator)
        if msg_type == InfoType.DISK.value:
            sid = highest_consumer.get(Mc.FIELD_FOLDER, None)  # take folder as sid
        else:
            sid = highest_consumer.get(Mc.FIELD_SID, None)

        mem_usage = highest_consumer[Mc.FIELD_USAGE]
        return email, employee_name, user_name, sid, mem_usage

    def run(self):
        """run the thread"""
        while True:
            consumer = Ku.get_consumer(Mc.MONITOR_GROUP_ID_ALARM)  # should be in different group with others
            consumer.assign(Ku.get_assignments(consumer, Mc.TOPIC_CONFIGURATION))
            # consumer.subscribe(Mc.TOPIC_CONFIGURATION)
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
            db_output = self.__monitor_dao.get_email_admin(server_id)
            try:
                administrators = [admin[0] for admin in db_output]
            except Exception as ex:
                administrators = []
                Mu.log_warning(self.__logger, "Parsing DB output failed in 'get_email_admin' "
                                              "with error: {0}, the output: {1}".format(ex, db_output))
            return administrators

        def get_users_info(self, server_id, users):
            db_output = self.__monitor_dao.get_info_by_5_sidadm_users(server_id, users)
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
