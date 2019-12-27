import logging
import logging.config
import smtplib
import configparser
import rsa
import binascii
import os
import traceback
import json
from errors import MonitorUtilError
from datetime import datetime
from enum import Enum
from contextlib import contextmanager
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.structs import TopicPartition


class InfoType(Enum):
    """Message info type for different resources"""
    MEMORY = 1
    CPU = 2
    DISK = 3
    INSTANCE = 4


class ActionType(Enum):
    """Action type for application operator"""
    SHUTDOWN = 1
    START = 2
    RESTART = 3
    CLEAN_LOG_BACKUP = 4


class MonitorConst:
    # ------Get configuration------
    __config = configparser.ConfigParser()
    __config.read(os.path.join(os.path.dirname(os.path.abspath(__file__)), "config/configuration.ini"))
    # ------ logger ------
    LOGGER_MONITOR_CONFIG_MGR = "monitor.config_mgr"
    LOGGER_MONITOR_COORDINATOR = "monitor.coordinator"
    LOGGER_MONITOR_ANALYZER = "monitor.analyzer"
    LOGGER_MONITOR_OPERATOR = "monitor.operator"
    LOGGER_MONITOR_OPERATOR_DB = "monitor.operator.db"
    LOGGER_MONITOR_OPERATOR_ALARM = "monitor.operator.alarm"
    LOGGER_MONITOR_OPERATOR_APP = "monitor.operator.app"

    LOGGER_AGENT = "monitor.agent"
    LOGGER_AGENT_MSG_PRODUCER = "monitor.agent.producer"
    LOGGER_MONITOR_MEM = "monitor.agent.mem"
    LOGGER_MONITOR_DISK = "monitor.agent.disk"
    LOGGER_MONITOR_CPU = "monitor.agent.cpu"
    LOGGER_MONITOR_INSTANCE = "monitor.agent.instance"

    LOGGER_MONITOR_EXTENSION = "monitor.extension"
    LOGGER_MONITOR_SERVER_OS_OPERATOR = "monitor.os_operator"
    LOGGER_MONITOR_SERVER_DB_OPERATOR = "monitor.db_operator"
    LOGGER_MONITOR_TEST = "monitor.test"
    LOGGER_MONITOR_UTILITY = "monitor.utility"
    LOGGER_MONITOR_EMAIL = "monitor.email"
    LOGGER_MONITOR_KAFKA = "monitor.kafka"
    LOGGER_MONITOR_INIT_SERVER = "monitor.init.server"
    LOGGER_MONITOR_INIT_SID = "monitor.init.sid"
    LOGGER_MONITOR_INIT_TABLE = "monitor.init.table"
    LOGGER_HANA_DB = "db.hana"
    LOGGER_OS_LINUX = "os.linux"

    # ------ local Const Values ------
    SERVER_INFO_MEM = "memory_info"
    SERVER_INFO_CPU = "cpu_info"
    SERVER_INFO_DISK = "disk_info"
    SERVER_INFO_OS = "os_info"

    MONITOR_STATUS_IN_PROCESS = "IN PROCESS"
    MONITOR_STATUS_COMPLETE = "COMPLETE"
    MONITOR_STATUS_ERROR = "ERROR"
    MONITOR_STATUS_WARNING = "WARNING"
    MONITOR_STATUS_SERVER_ID_OVER_ALL = -1
    MONITOR_STAGE_OVERALL = -1
    MONITOR_STAGE_MEM = 1
    MONITOR_STAGE_CPU = 2
    MONITOR_STAGE_DISK = 3
    MONITOR_STAGE_INSTANCE = 4
    # ------ DB field mapping ------
    FIELD_SERVER_ID = "SERVER_ID"
    FIELD_SERVER_FULL_NAME = "SERVER_FULL_NAME"
    FIELD_MOUNT_POINT = "MOUNT_POINT"
    FIELD_DISK_TOTAL = "DISK_TOTAL"
    FIELD_DISK_FREE = "DISK_FREE"
    FIELD_MEM_TOTAL = "MEM_TOTAL"
    FIELD_MEM_FREE = "MEM_FREE"
    FIELD_CPU_NUMBER = "CPU_NUMBER"
    FIELD_CPU_UTILIZATION = "CPU_UTILIZATION"
    FIELD_OS = "OS"
    FIELD_KERNEL = "KERNEL"
    FIELD_CHECK_TIME = "CHECK_TIME"
    FIELD_CHECK_ID = "CHECK_ID"
    FIELD_USER_NAME = "USER_NAME"
    FIELD_EMPLOYEE_NAME = "EMPLOYEE_NAME"
    FIELD_EMAIL = "EMAIL"
    FIELD_USAGE = "USAGE"
    FIELD_PROCESS_COMMAND = "PROCESS_COMMAND"
    FIELD_PROCESS_ID = "PROCESS_ID"
    FIELD_CPU = "CPU"
    FIELD_MEM = "MEM"
    FIELD_DISK_USAGE_KB = "DISK_USAGE_KB"
    FIELD_FOLDER = "FOLDER"
    FIELD_SID = "SID"
    FIELD_SID_START = "SID_START"
    FIELD_SID_END = "SID_END"
    FIELD_FILTER_FLAG = "FILTER_FLAG"
    FIELD_EMPLOYEE_ID = "EMPLOYEE_ID"
    FIELD_LOCATION_ID = "LOCATION_ID"
    FIELD_LOCATION = "LOCATION"
    FIELD_REVISION = "REVISION"
    FIELD_INSTANCE_NO = "INSTANCE_NUM"
    FIELD_HOST = "HOST"
    FIELD_EDITION = "EDITION"

    # -- Kafka
    TOPIC_CONFIGURATION = "configuration"
    TOPIC_MONITORING_INFO = "monitoring_info"
    TOPIC_AGENT_HEARTBEAT = "monitoring_agent_heartbeat"
    TOPIC_FILTERED_INFO = "monitoring_filtered_data"
    TOPIC_APP_OPERATION = "app_operation"
    MONITOR_GROUP_ID = "monitor_group"
    MONITOR_GROUP_ID_AGENT = "monitor_group_alarm"
    MONITOR_GROUP_ID_ALARM = "monitor_group_alarm"
    MONITOR_GROUP_ID_ALARM_HEARTBEAT = "monitor_group_alarm_heartbeat"
    MONITOR_GROUP_ID_ANALYZER = "monitor_group_analyzer"
    MONITOR_GROUP_ID_APP_OPERATOR = "monitor_group_app_operator"
    MONITOR_GROUP_ID_CONFIG = "monitor_group_config"
    MONITOR_GROUP_ID_COORDINATOR = "monitor_group_coordinator"
    MONITOR_GROUP_ID_DB_OPERATOR = "monitor_group_db_operator"

    MSG_TYPE = "type"
    MSG_INFO = "info"
    MSG_HEADER = "header"
    MSG_ENDING = "ending"
    MSG_TIME = "message_time"
    # MSG_HEARTBEAT = "heartbeat"
    INFO_TOTAL = "total"
    INFO_FREE = "free"
    INFO_USAGE = "usage"
    INFO_ALARM_NUM = "alarm_num"
    INFO_ALARM_TIME = "alarm_time"
    # -- db configuration
    _DB_CONFIGURATION_COMPONENT_GLOBAL = "GLOBAL"
    _DB_CONFIGURATION_COMPONENT_MONITOR = "MONITOR"

    DB_CONFIGURATION_SERVER = "SERVER"

    # ------ get values from configuration file ------
    @staticmethod
    def get_test_mode():
        """get test mode from configuration, this is an optional config, the default value is false"""
        try:
            test_mode = MonitorConst.__config.get("monitor", "test_mode")
        except configparser.NoOptionError:
            test_mode = "false"

        return test_mode

    @staticmethod
    def get_email_admin():
        return MonitorConst.__config.get("monitor", "email_administrators").split(",")

    @staticmethod
    def get_sidadmin_default_password():
        return MonitorConst.__config.get("monitor.os", "sidadm_default_password")

    @staticmethod
    def get_ssh_default_user():
        return MonitorConst.__config.get("monitor.os", "ssh_default_user")

    @staticmethod
    def get_ssh_default_password():
        return MonitorConst.__config.get("monitor.os", "ssh_default_password")

    @staticmethod
    def get_ssh_default_os_type():
        return MonitorConst.__config.get("monitor.os", "ssh_default_os_type")

    @staticmethod
    def get_rsa_key_file():
        return MonitorConst.__config.get("monitor.os", "rsa_key_file")

    @staticmethod
    def use_simulator_4_mem():
        return MonitorConst.__config.getboolean("monitor.os.simulator", "use_simulator_4_memory")

    @staticmethod
    def use_simulator_4_disk():
        return MonitorConst.__config.getboolean("monitor.os.simulator", "use_simulator_4_disk")

    @staticmethod
    def use_simulator_4_cpu():
        return MonitorConst.__config.getboolean("monitor.os.simulator", "use_simulator_4_cpu")

    @staticmethod
    def get_init_sql_file():
        return os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            MonitorConst.__config.get("monitor.db.initializer", "initialize_sql_script_file"))

    @staticmethod
    def get_hana_server():
        return MonitorConst.__config.get("monitor.db", "hana_server")

    @staticmethod
    def get_hana_port():
        return MonitorConst.__config.getint("monitor.db", "hana_port")

    @staticmethod
    def get_hana_user():
        return MonitorConst.__config.get("monitor.db", "hana_user")

    @staticmethod
    def get_hana_password():
        return MonitorConst.__config.get("monitor.db", "hana_password")

    @staticmethod
    def get_kafka_server():
        return MonitorConst.__config.get("monitor.mb", "kafka_server")

    @staticmethod
    def get_kafka_port():
        return MonitorConst.__config.get("monitor.mb", "kafka_port")

    @staticmethod
    def get_agent_path():
        return MonitorConst.__config.get("monitor.agent", "agent_path")

    @staticmethod
    def get_smtp_server():
        return MonitorConst.__config.get("monitor.alarm", "smtp_server")

    @staticmethod
    def get_smtp_port():
        return MonitorConst.__config.get("monitor.alarm", "smtp_port")

    @staticmethod
    def get_email_sender():
        return MonitorConst.__config.get("monitor.alarm", "email_sender")

    @staticmethod
    def get_email_password():
        return MonitorConst.__config.get("monitor.alarm", "email_password")

    @staticmethod
    def get_heartbeat_check_interval():
        return int(MonitorConst.__config.get("monitor.heartbeat", "check_interval"))

    @staticmethod
    def get_heartbeat_timeout():
        return int(MonitorConst.__config.get("monitor.heartbeat", "timeout_s"))

    @staticmethod
    def get_heartbeat_operation_interval():
        return int(MonitorConst.__config.get("monitor.heartbeat", "operation_interval"))

    @staticmethod
    def get_heartbeat_email_interval():
        return int(MonitorConst.__config.get("monitor.heartbeat", "email_interval"))

    @staticmethod
    def get_app_operation_check_interval():
        return int(MonitorConst.__config.get("monitor.app.operation", "check_interval"))

    @staticmethod
    def __get_db_configuration_not_real_time(name, component, db_operator, logger=None):
        """If local variable exists, get configuration from local variable.
        Otherwise, get the configuration from database and save the configuration to local variable.
        Will raise exception if get configuration from database failed.
        """
        variable_name = "_DB_{0}".format(name)
        config_value = getattr(MonitorConst, variable_name) if hasattr(MonitorConst, variable_name) else None
        if config_value is not None:
            return config_value

        config_value = db_operator.get_db_configuration(component, name)
        if logger:
            logger.debug("Got configuration: ({0},{1}) From DB, value is {2}.".format(component, name, config_value))

        if config_value is None:
            message = "Getting DB configuration: ({0},{1}) failed.".format(component, name)
            if logger:
                logger.error(message)
            raise MonitorUtilError(message)
        setattr(MonitorConst, variable_name, config_value)
        return config_value

    @staticmethod
    def get_operation_hours(logger=None):
        """get the operation time from attributes directly, not from database.
        Before calling this function, get_db_operation_time need to be called first to load the values
        from database to attributes"""
        variable_name = "_DB_{0}".format("OPERATION_TIME")
        config_value = getattr(MonitorConst, variable_name) if hasattr(MonitorConst, variable_name) else None
        if config_value is not None:
            try:
                operation_hour_start, operation_hour_end = config_value.split(',')
                return int(operation_hour_start), int(operation_hour_end)
            except Exception as ex:
                if logger:
                    logger.warning("Parsing operation hours failed in 'get_operation_hours' with error: {0}".format(ex))
                return 7, 19
        else:
            # get_db_operation_time did not get called, use default value
            return 7, 19

    @staticmethod
    def get_db_operation_time(db_operator, logger=None):
        return MonitorConst.__get_db_configuration_not_real_time("OPERATION_TIME",
                                                                 MonitorConst._DB_CONFIGURATION_COMPONENT_MONITOR,
                                                                 db_operator,
                                                                 logger)

    @staticmethod
    def get_db_email_sender(db_operator, logger=None):
        return MonitorConst.__get_db_configuration_not_real_time("EMAIL_SENDER",
                                                                 MonitorConst._DB_CONFIGURATION_COMPONENT_MONITOR,
                                                                 db_operator,
                                                                 logger)

    @staticmethod
    def get_db_cpu_usage_warn_threshold(db_operator, logger=None):
        return MonitorConst.__get_db_configuration_not_real_time("THRESHOLD_CPU_USAGE_WARN_INT",
                                                                 MonitorConst._DB_CONFIGURATION_COMPONENT_GLOBAL,
                                                                 db_operator,
                                                                 logger)

    @staticmethod
    def get_db_mem_usage_warn_threshold(db_operator, logger=None):
        return MonitorConst.__get_db_configuration_not_real_time("THRESHOLD_MEM_USAGE_WARN_INT",
                                                                 MonitorConst._DB_CONFIGURATION_COMPONENT_GLOBAL,
                                                                 db_operator,
                                                                 logger)

    @staticmethod
    def get_db_disk_usage_warn_threshold(db_operator, logger=None):
        return MonitorConst.__get_db_configuration_not_real_time("THRESHOLD_DISK_USAGE_WARN_INT",
                                                                 MonitorConst._DB_CONFIGURATION_COMPONENT_GLOBAL,
                                                                 db_operator,
                                                                 logger)

    @staticmethod
    def get_db_check_interval(db_operator, logger=None):
        return MonitorConst.__get_db_configuration_not_real_time("CHECK_INTERVAL_INT",
                                                                 MonitorConst._DB_CONFIGURATION_COMPONENT_MONITOR,
                                                                 db_operator,
                                                                 logger)

    @staticmethod
    def get_db_max_failure_times(db_operator, logger=None):
        return MonitorConst.__get_db_configuration_not_real_time("MAX_FAILURE_TIMES_INT",
                                                                 MonitorConst._DB_CONFIGURATION_COMPONENT_MONITOR,
                                                                 db_operator,
                                                                 logger)

    @staticmethod
    def is_user_sudoer(logger=None):
        # currently only support two types of groups: sudo or sapsys
        # if user is not in the sudoers, use must be included in sapsys group
        # use sapsys group may cause a little bit lower value when collecting HANA instance disk consumption,
        # because some security folders do not have read access to sapsys group
        try:
            user_group = MonitorConst.__config.get("monitor.os", "ssh_default_user_group")
        except configparser.Error as ex:
            logger.warning("Getting configuration:(monitor.os, ssh_default_user_group) failed:{0}. "
                           "Will use default value)".format(ex))
            user_group = "sapsys"
        return user_group == "sudo"


class MonitorUtility:
    """Utilities of monitor, providing the functions to get the value from configuration and some tools
    gen_sid_list: Generate all the SIDs by the given sid_start and sid_end
    send_email: Send email via mail.sap.corp
    """
    try:
        logging.config.fileConfig(os.path.join(os.path.dirname(os.path.abspath(__file__)), "config/logging.ini"))
        __logger = logging.getLogger(MonitorConst.LOGGER_MONITOR_UTILITY)
    except FileNotFoundError as ex:
        print("Initializing logger failed with exception {0}".format(ex))

    @staticmethod
    def is_float(string_to_check):
        is_float = True
        try:
            float(string_to_check)
        except ValueError:
            is_float = False
        return is_float

    @staticmethod
    def get_logger(model_name="monitor"):
        return logging.getLogger(model_name)

    @staticmethod
    def is_test_mod():
        test_mode = MonitorConst.get_test_mode()
        return test_mode.lower() in ['true', '1', 'yes'] if test_mode else False

    @staticmethod
    def gen_sid_list(sid_start, sid_end):
        """Generate all the SIDs by the given sid_start and sid_end
         eg: sid_start = CK0, sid_end = CK3 generate the sid list that contains:[CK0, CK1, CK2, CK3]
         SID mapping only supports following patterns:
            [A-Z][A-Z][A-Z] ~ [A-Z][A-Z][A-Z]:first two chars should be identical, eg: CKA ~ CKW
            [A-Z][0-9][A-Z] ~ [A-Z][0-9][A-Z]:first two chars should be identical, eg: C3A ~ C3W
            [A-Z][A-Z][0-9] ~ [A-Z][A-Z][A-Z]:first two chars should be identical, eg: CK2 ~ CKH
            [A-Z][A-Z][0-9] ~ [A-Z][A-Z][0-9]:first two chars should be identical, eg: CK2 ~ CK8
            [A-Z][0-9][0-9] ~ [A-Z][0-9][0-9]:first char should be identical, eg: C02 ~ C68
         """
        sid_start_part1, sid_start_part2 = MonitorUtility.__get_sid_part1_part2(sid_start)
        sid_end_part1, sid_end_part2 = MonitorUtility.__get_sid_part1_part2(sid_end)

        # currently only support sid range end with 1 non-digit
        # eg: sid_start = C11, sid_end = C1Z --> change sid_start_part1 from C to C1, sid_start_part2 from 11 to 1
        if not isinstance(sid_end_part2, int) and len(sid_end_part1) == 2 \
                and (isinstance(sid_start_part2, int) and len(sid_start_part1) == 1):
            sid_start_part1 = "".join([sid_start_part1, str(int(sid_start_part2 / 10))])
            sid_start_part2 = sid_start_part2 % 10

        sid_list = []

        if sid_start_part1 is None or sid_start_part1 != sid_end_part1:
            MonitorUtility.__logger.error(
                "SID Mapping Error, SID_START={0}, SID_END={1}".format(sid_start, sid_end))
            MonitorUtility.__logger.info(
                """Note: SID mapping only supports following patterns:
                [A-Z][A-Z][A-Z] ~ [A-Z][A-Z][A-Z]:first two chars should be identical, eg: CKA ~ CKW
                [A-Z][0-9][A-Z] ~ [A-Z][0-9][A-Z]:first two chars should be identical, eg: C3A ~ C3W
                [A-Z][A-Z][0-9] ~ [A-Z][A-Z][A-Z]:first two chars should be identical, eg: CK2 ~ CKH
                [A-Z][A-Z][0-9] ~ [A-Z][A-Z][0-9]:first two chars should be identical, eg: CK2 ~ CK8
                [A-Z][0-9][0-9] ~ [A-Z][0-9][0-9]:first char should be identical, eg: C02 ~ C68""")
            return sid_list

        if isinstance(sid_start_part2, int) and isinstance(sid_end_part2, int):
            # eg: C01 ~ C99  or CK1 ~ CK9
            for num in range(sid_start_part2, sid_end_part2 + 1):
                sid_str_len = len(sid_start_part1)
                sid = "".join([sid_start_part1, str(num)]) \
                    if (sid_str_len > 1 and num < 10) or (sid_str_len == 1 and num >= 10) \
                    else "".join([sid_start_part1, "0", str(num)])
                sid_list.append(sid)
        elif isinstance(sid_start_part2, int) and not isinstance(sid_end_part2, int):
            # eg: CK1 ~ CKZ or C31 ~ C3Z
            for num in range(sid_start_part2, 10):  # gen the number part, eg: CK1 ~ CK9 or C31 ~ C39
                sid_str_len = len(sid_start_part1)
                sid = "".join([sid_start_part1, str(num)]) \
                    if (sid_str_len > 1 and num < 10) or (sid_str_len == 1 and num >= 10) \
                    else "".join([sid_start_part1, "0", str(num)])
                sid_list.append(sid)

            for x in range(ord('A'), ord(sid_end_part2) + 1):  # gen the non-digit part, eg: CKA ~ CKZ or C3A ~ C3Z
                sid = "".join([sid_start_part1, chr(x)])
                sid_list.append(sid)
        elif not isinstance(sid_start_part2, int) and not isinstance(sid_end_part2, int):
            # eg: CKA ~ CKZ or C0A ~ C0Y
            for x in range(ord(sid_start_part2), ord(sid_end_part2) + 1):
                sid = "".join([sid_start_part1, chr(x)])
                sid_list.append(sid)

        return sid_list

    @staticmethod
    def __get_sid_part1_part2(sid):
        """Get the part1 and part 2 of the given SID"""
        if sid is not None and len(sid) == 3:
            sid = sid.upper()
            if not sid[0].isdigit():
                if sid[1:3].isdigit():  # 1 Char + 2 Num
                    sid_part1 = sid[0:1]
                    sid_part2 = int(sid[1:3])
                    return sid_part1, sid_part2
                elif sid[2:3].isdigit():  # 2 Chars + 1 Num
                    sid_part1 = sid[0:2]
                    sid_part2 = int(sid[2:3])
                    return sid_part1, sid_part2
                else:  # 2 Chars + 1 Char
                    sid_part1 = sid[0:2]
                    sid_part2 = sid[2:3]
                    return sid_part1, sid_part2

        MonitorUtility.__logger.error("SID Format Error, SID=%s" % sid)

    @staticmethod
    def get_sid_from_sidadm(sidadm):
        """get the sid from sidadm
        ck1adm -> return CK1
        ckadm -> return CK
        ck123adm -> return ck123
        share -> return SHARE"""
        return sidadm[:-len("adm")].upper() if sidadm is not None and sidadm.lower().endswith("adm") else sidadm

    @staticmethod
    def get_sidadm_from_sid(sid):
        return "".join([sid.lower(), "adm"])

    @staticmethod
    def is_current_time_working_time(operation_hours):
        current_time = datetime.now()
        hour = int(current_time.strftime('%H'))

        try:
            operation_hour_start, operation_hour_end = operation_hours.split(',')
            start_hour = int(operation_hour_start)
            end_hour = int(operation_hour_end)
        except Exception as ex:
            MonitorUtility.__logger.warning(
                "Parsing operation hours failed in 'is_current_time_working_time' with error: {0}".format(ex))
            start_hour = 7
            end_hour = 19

        weekday = current_time.weekday()
        return start_hour <= hour <= end_hour and weekday < 5

    @staticmethod
    def generate_public_private_key(path):
        """generate RSA public and private key to the specified path,
        public key will be the file: monitor_ssh_user_public.pem
        private key will be the file: monitor_ssh_user_private.pem"""
        public_key_file_name = "monitor_ssh_user_public.pem"
        private_key_file_name = "monitor_ssh_user_private.pem"
        (public_key, private_key) = rsa.newkeys(4096, poolsize=8)
        public_key_byte = public_key.save_pkcs1()
        import os
        with open(os.path.join(path, public_key_file_name), 'wb') as public_file:
            public_file.write(public_key_byte)

        private_key_byte = private_key.save_pkcs1()
        with open(os.path.join(path, private_key_file_name), 'wb') as private_file:
            private_file.write(private_key_byte)

        print("File generated to {0}! Public key file: {1}, private key file: {2}".format(path,
                                                                                          public_key_file_name,
                                                                                          private_key_file_name))

    @staticmethod
    def get_encrypt_string(public_key_path, string_to_encrypt):
        with open(public_key_path, mode='rb') as public_file:
            key_data = public_file.read()
            public_data = rsa.PublicKey.load_pkcs1(key_data)
            string_to_encrypt = string_to_encrypt.encode("utf-8")
            return binascii.b2a_base64(rsa.encrypt(string_to_encrypt, public_data)).decode('utf-8')

    @staticmethod
    def get_decrypt_string(private_key_path, string_to_decrypt):
        byte_to_decrypt = binascii.a2b_base64(string_to_decrypt)
        with open(private_key_path, mode='rb') as private_file:
            key_data = private_file.read()
            private_data = rsa.PrivateKey.load_pkcs1(key_data)
            return rsa.decrypt(byte_to_decrypt, private_data).decode("utf-8")

    @staticmethod
    def generate_check_id():
        return datetime.now().strftime('%Y%m%d%H%M%S%f')

    @staticmethod
    def get_time_via_check_id(check_id):
        return datetime.strptime(check_id, '%Y%m%d%H%M%S%f')

    @staticmethod
    @contextmanager
    def open_ssh_connection(logger, operator, server_name, user_name, user_password):
        ssh = None
        try:
            MonitorUtility.log_debug(logger, "Trying to connect {0} with user {1}.".format(server_name, user_name))
            ssh = operator.open_ssh_connection(server_name, user_name, user_password)
            if ssh is not None:
                MonitorUtility.log_debug(logger, "Connected {0}.".format(server_name))
            yield ssh
        finally:
            MonitorUtility.close_ssh_connection(operator, ssh, logger, server_name)

    @staticmethod
    def close_ssh_connection(operator, ssh, logger=None, server_name=None):
        if logger is not None:
            MonitorUtility.log_debug(logger, "Trying to close connection to {0}.".format(server_name))
        else:
            MonitorUtility.log_debug(logger, "Trying to close connection")
        operator.close_ssh_connection(ssh)
        if logger is not None:
            if server_name is not None:
                MonitorUtility.log_debug(logger, "connection to {0} is closed.".format(server_name))
            else:
                MonitorUtility.log_debug(logger, "connection is closed.")

    @staticmethod
    def process_heartbeat(logger, heartbeat_info, consumer, timeout, failure_action):
        heartbeat_msg_pack = consumer.poll(update_offsets=True)
        if heartbeat_msg_pack:
            for tp, messages in heartbeat_msg_pack.items():
                for message in messages:
                    # Mc.FIELD_SERVER_ID: server_id, Mc.MSG_TYPE: info_type, Mc.MSG_TIME: datetime.now()})
                    server_id = message.value[MonitorConst.FIELD_SERVER_ID]
                    msg_type = message.value[MonitorConst.MSG_TYPE]
                    msg_time = message.value[MonitorConst.MSG_TIME]
                    # update heartbeat info
                    try:
                        heartbeat_info[server_id][msg_type] = MonitorUtility.get_time_via_check_id(msg_time)
                    except Exception as ex:
                        MonitorUtility.log_warning(logger, ("Converting heartbeat time failed with: {0}. Time is {1}, "
                                                            "message is: {2}").format(ex, msg_time, message.value))

        # no matter there is new message or not, perform heartbeat check anyway
        MonitorUtility.__check_heartbeat_info(logger, heartbeat_info, timeout, failure_action)
        MonitorUtility.log_debug(logger, "Processing heartbeat is finished.")

    @staticmethod
    def __check_heartbeat_info(logger, info, timeout, failure_action):
        cur_time = datetime.now()
        # currently, do not consider different type of resource timeout, just use an over all status...
        for server_id, type_time in info.items():
            pre_time = datetime.min
            for info_type, process_time in type_time.items():
                pre_time = max(pre_time, process_time)

            MonitorUtility.log_debug(logger, ("Heartbeat checking for {0}, current time is {1}, heartbeat "
                                              "time is {2}").format(server_id, cur_time, pre_time))
            if (cur_time - pre_time).total_seconds() >= timeout:
                MonitorUtility.log_info(logger, "Heartbeat timeout for {0}".format(server_id))
                failure_action(server_id)

    @staticmethod
    def __get_log_message(message):
        return "[{0}] {1}".format(os.getpid(), message)

    @staticmethod
    def log_debug(logger, message):
        if logger is not None:
            try:
                logger.debug(MonitorUtility.__get_log_message(message))
            except Exception as ex:
                print("Logging [debug] message '{0}' failed, error:{1}".format(message, ex))
        else:
            print(message)

    @staticmethod
    def log_info(logger, message):
        if logger is not None:
            try:
                logger.info(MonitorUtility.__get_log_message(message))
            except Exception as ex:
                print("Logging [info] message '{0}' failed, error:{1}".format(message, ex))
        else:
            print(message)

    @staticmethod
    def log_warning(logger, message):
        if logger is not None:
            try:
                logger.warning(MonitorUtility.__get_log_message(message))
            except Exception as ex:
                print("Logging [warning] message '{0}' failed, error:{1}".format(message, ex))
        else:
            print(message)

    @staticmethod
    def log_error(logger, message):
        if logger is not None:
            try:
                logger.error(MonitorUtility.__get_log_message(message))
            except Exception as ex:
                print("Logging [error] message '{0}' failed, error:{1}".format(message, ex))
        else:
            print(message)

    @staticmethod
    def log_exception(logger, message=None):
        if logger is not None:
            try:
                if message is None:
                    logger.exception(traceback.format_exc())
                else:
                    logger.exception(MonitorUtility.__get_log_message(message))
            except Exception as ex:
                print("Logging [exception] message '{0}' failed, error:{1}".format(message, ex))
        else:
            print(message)

    @staticmethod
    def log_warning_exc(logger, message):
        MonitorUtility.log_warning(logger, message)
        MonitorUtility.log_exception(logger)

    @staticmethod
    def log_error_exc(logger, message):
        MonitorUtility.log_error(logger, message)
        MonitorUtility.log_exception(logger)


class Email:
    """class for sending email"""
    __logger = logging.getLogger(MonitorConst.LOGGER_MONITOR_EMAIL)
    @staticmethod
    def __get_email_body_for_shutdown(name, server, sid, usage, emergency=False):
        emergency_str = 'EMERGENCY ' if emergency else ''
        email_body = ("Dear {0}, \n\n{1} is running out of memory, the [{4}SHUTDOWN] of your {2} is "
                      "because of its consuming highest memory ({3}%). "
                      "If this SID is very important and you do not want "
                      "it to be shut down next time, please contact administrator"
                      " to mark it as an important SID. \n"
                      "\n\nRegards,\nHANA OS Monitor".format(name, server, sid, round(usage, 2), emergency_str))
        return email_body

    @staticmethod
    def send_emergency_shutdown_email(sender, receiver, sid, server_name, employee_name, admin, usage, info_type):
        if receiver is not None and len(receiver) > 0:
            # sending email to the owner of the instance
            email_to = [receiver]
            email_body = Email.__get_email_body_for_shutdown(employee_name, server_name, sid, usage, True)
            Email.send_email(
                sender,
                email_to,
                "[MONITOR.{0}][EMERGENCY] {1} on {2} is Shutting Down".format(info_type.name, sid, server_name),
                email_body,
                admin)
        else:
            Email.__logger.warning("No emergency shutdown email is sent out, because of the empty email receiver.")

    @staticmethod
    def send_shutdown_email(sender, receiver, sid, server_name, employee_name, admin, usage, info_type):
        """ Sending email to the owner of the instance that consuming the highest memory for shutting down HANA"""
        if receiver is not None and len(receiver) > 0:
            # sending email to the owner of the instance
            email_to = [receiver]
            email_body = Email.__get_email_body_for_shutdown(employee_name, server_name, sid, usage, False)

            Email.send_email(
                sender,
                email_to,
                "[MONITOR.{0}] {1} on {2} is Shutting Down".format(info_type.name, sid, server_name),
                email_body,
                admin)
        else:
            Email.__logger.warning("No shutdown email is sent out, because of the empty email receiver.")

    @staticmethod
    def send_cleaning_disk_email(sender, receiver, sid, server_name, employee_name, admin, usage, info_type):
        """ Sending email to the owner of the instance that consuming the highest disk space
        for log backup cleaning"""
        if receiver is not None and len(receiver) > 0:
            email_to = [receiver]

            email_body = ("Dear {0}, \n\n{1} is running out of disk space, your {2} is taking the highest amount of "
                          "disk space ({3} GB). There will be a clean up process for your log backup automatically, "
                          "all the log backups which older than 10 days will be deleted. \n.\n\nRegards,"
                          "\nHANA OS Monitor".format(employee_name, server_name, sid, round(usage / 1024 / 1024, 2)))
            Email.send_email(
                sender,
                email_to,
                "[MONITOR.{0}] Log Backup of {1} on {2} is Cleaning".format(info_type.name, sid, server_name),
                email_body,
                admin)
        else:
            Email.__logger.warning("No email is sent out, because of the empty email receiver.")

    @staticmethod
    def send_warning_email(sender, receiver, info_type, server_name, consumers, admin):
        if receiver is not None and len(receiver) > 0:
            Email.send_email(sender,
                             receiver,
                             Email.generate_email_subject(info_type, server_name),
                             Email.generate_email_body(info_type, consumers),
                             admin)
        else:
            Email.__logger.warning("No email is sent out, because of the empty email receiver.")

    @staticmethod
    def send_heartbeat_failure_email(sender, server_name, admin):
        email_subject = "[MONITOR.HEARTBEAT] No Heartbeat from {0}".format(server_name)
        email_body = "Heartbeat checking timed out on server {0}, please check the status of that server.".format(
            server_name)
        Email.send_email(sender, None, email_subject, email_body, admin)

    @staticmethod
    def send_email(email_from,
                   email_to,
                   email_subject,
                   email_content,
                   email_cc=None,
                   email_password=None,
                   smtp_server=None,
                   smtp_port=None):
        """Tool for sending email.
        email_from: string
        email_to: list, doesn't support string
        email_subject: string
        email_content: string
        email_cc: list, default value is None
        email_password: string, default value is None (for those SMTP servers which do not need password)"""

        if email_to is None:
            email_to = []
        if email_cc is None:
            email_cc = []
        if not isinstance(email_to, list) or not isinstance(email_cc, list):
            Email.__logger.error("String is not accepted for the recipients! Please use list instead!")
            return
        # remove duplicates from email_to and email_cc
        email_to = list(set(email_to))
        email_cc = list(set(email_cc))
        server = None
        try:
            if not smtp_server:
                smtp_server = MonitorConst.get_smtp_server()
            if not smtp_port:
                smtp_port = MonitorConst.get_smtp_port()
            if not email_password:
                try:
                    email_password = MonitorConst.get_email_password()
                except Exception as ex:
                    Email.__logger.error("Retrieve email password failed at {0},use empty password instead.".format(ex))
                    email_password = None

            if not email_from:
                email_from = MonitorConst.get_email_sender()

            server = smtplib.SMTP(smtp_server, smtp_port)
            if email_password is not None and len(email_password) > 0:
                server.login(email_from, email_password)

            email_body = "\r\n".join(["From: {0}".format(email_from),
                                      "To: {0}".format(",".join(email_to)),
                                      "CC: {0}".format(",".join(email_cc)),
                                      "Subject: {0}".format(email_subject),
                                      "",
                                      email_content])

            server.sendmail(email_from, email_to + email_cc, email_body)
            Email.__logger.info("Email:{0} is sent to {1} successfully!".format(email_subject, email_to + email_cc))
        except Exception as ex:
            Email.__logger.error("Failed to send email to {0}! Error:{1}, Subject:".format(email_to, ex, email_subject))
        finally:
            if server is not None:
                server.quit()

    @staticmethod
    def generate_email_subject(info_type, server_name):
        if info_type == InfoType.MEMORY:
            info = "Memory"
        elif info_type == InfoType.CPU:
            info = "CPU Resource"
        elif info_type == InfoType.DISK:
            info = "Disk Space"
        else:
            info = ""
        return "[MONITOR.{0}] {1} is Running Out of {2}".format(info_type.name, server_name, info)

    @staticmethod
    def generate_email_body(email_type, info):
        if email_type == InfoType.MEMORY:
            total = round(info[MonitorConst.INFO_TOTAL] / 1024 / 1024, 2)
            free = round(info[MonitorConst.INFO_FREE] / 1024 / 1024, 2)
        elif email_type == InfoType.DISK:
            total = round(info[MonitorConst.INFO_TOTAL] / 1024 / 1024, 2)
            free = round(info[MonitorConst.INFO_FREE] / 1024 / 1024, 2)
        else:  # MonitorConst.SERVER_INFO_CPU
            total = round(100 - info[MonitorConst.INFO_FREE], 2)  # use total as cpu usage: 100 - free cpu
            free = 0

        server_name = info[MonitorConst.FIELD_SERVER_FULL_NAME]

        check_id = info[MonitorConst.FIELD_CHECK_ID]
        check_time = MonitorUtility.get_time_via_check_id(check_id)

        body = ("Server Name: {0} \n"
                "\t CPU Utilization: {1} % \n"
                "\t Check Time: {2}".format(server_name, total, check_time)) \
            if email_type == InfoType.CPU else (
            "Server Name:{0}\n"
            "\t Total {1}: {2} GB\n"
            "\t Free {3}: {4} GB\n"
            "\t Check Time: {5}".format(server_name, email_type.name, total, email_type.name, free, check_time))

        # top 5 consumers
        try:
            body_additional = "\n Following is the top 5 {0} consumers, check id:{1}:\n ".format(email_type.name,
                                                                                                 check_id)
            unit_type = "GB" if email_type == InfoType.DISK else "%"
            for consumer in info[MonitorConst.INFO_USAGE]:
                if email_type == InfoType.DISK:
                    consuming_info = "Folder: {0}".format(consumer[MonitorConst.FIELD_FOLDER])
                    usage = round(consumer[MonitorConst.FIELD_USAGE] / 1024 / 1024, 2)
                else:
                    consuming_info = "SID: {0}".format(consumer[MonitorConst.FIELD_SID])
                    usage = round(consumer[MonitorConst.FIELD_USAGE], 2)

                body_additional = "".join(
                    [body_additional, "\t {0}, Name: {1}, {2} Usage: {3} {4}\n".format(
                        consuming_info,
                        consumer.get(MonitorConst.FIELD_EMPLOYEE_NAME, None),
                        email_type.name,
                        usage,
                        unit_type)])
            body = "".join([body, body_additional])
        except Exception as ex:
            Email.__logger.warning(
                "Error occurred when generate email body for top 5 {0} consumers on {1}, error is {2}".format(
                    email_type.name, server_name, ex))
        # TODO: Will add more inform like: "This is the {0} warning email, the HANA instance which
        # consuming the most Memory will be shutdown after THREE warning email!!"
        return body


class KafKaUtility:
    __logger = logging.getLogger(MonitorConst.LOGGER_MONITOR_KAFKA)

    @staticmethod
    def get_assignments(consumer, *topics):
        assignments = []
        for topic in topics:
            partitions = consumer.partitions_for_topic(topic)
            for p in partitions:
                assignments.append(TopicPartition(topic, p))

        return assignments

    @staticmethod
    def get_topic_partitions(consumer, topic):
        partitions = consumer.partitions_for_topic(topic)
        return [TopicPartition(topic, p) for p in partitions]

    @staticmethod
    def assign_and_seek_to_end(consumer, topic_to_seek, *topics):
        consumer.assign(KafKaUtility.get_assignments(consumer, *topics))
        consumer.seek_to_end(*KafKaUtility.get_topic_partitions(consumer, topic_to_seek))

    @staticmethod
    def get_producer():
        try:
            producer = KafkaProducer(
                bootstrap_servers=["{0}:{1}".format(MonitorConst.get_kafka_server(), MonitorConst.get_kafka_port())],
                value_serializer=lambda v: json.dumps(v).encode('ascii'))
            return producer
        except Exception as ex:
            err_msg = "Get producer failed with error: {0}".format(ex)
            MonitorUtility.log_error_exc(KafKaUtility.__logger, err_msg)
            raise MonitorUtilError(err_msg)

    @staticmethod
    def get_consumer(group_id, topic=None):
        try:
            if topic is not None:
                consumer = KafkaConsumer(
                    topic,
                    group_id=group_id,
                    bootstrap_servers=[
                        "{0}:{1}".format(MonitorConst.get_kafka_server(), MonitorConst.get_kafka_port())],
                    value_deserializer=lambda m: json.loads(m.decode('ascii')))
            else:
                consumer = KafkaConsumer(
                    group_id=group_id,
                    bootstrap_servers=[
                        "{0}:{1}".format(MonitorConst.get_kafka_server(), MonitorConst.get_kafka_port())],
                    value_deserializer=lambda m: json.loads(m.decode('ascii')))
            return consumer
        except Exception as ex:
            err_msg = "Get consumer failed with error: {0}".format(ex)
            MonitorUtility.log_error_exc(KafKaUtility.__logger, err_msg)
            raise MonitorUtilError(err_msg)
