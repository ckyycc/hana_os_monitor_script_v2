import logging
import logging.config
import smtplib
import configparser
import rsa
import binascii
import os
from errors import MonitorUtilError
from datetime import datetime
from enum import Enum


class InfoType(Enum):
    """Message info type for different resources"""
    MEMORY = 1
    CPU = 2
    DISK = 3
    INSTANCE = 4


class MonitorConst:

    # ------Get configuration------
    __config = configparser.ConfigParser()
    __config.read(os.path.join(os.path.dirname(os.path.abspath(__file__)), "config/configuration.ini"))
    # ------ logger ------
    LOGGER_MONITOR_CONFIG_MGR = "monitor.config_mgr"
    LOGGER_MONITOR_COORDINATOR = "monitor.coordinator"
    LOGGER_MONITOR_ANALYZER = "monitor.analyzer"
    LOGGER_MONITOR_OPERATOR = "monitor.operator"
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
    FIELD_EMPLOYEE_ID = "EMPLOYEE_ID"
    FIELD_LOCATION_ID = "LOCATION_ID"
    FIELD_LOCATION = "LOCATION"
    FIELD_REVISION = "REVISION"
    FIELD_INSTANCE_NO = "INSTANCE_NUM"
    FIELD_HOST = "HOST"
    FIELD_EDITION = "EDITION"

    # -- Kafka Topic
    TOPIC_CONFIGURATION = "configuration"
    TOPIC_SERVER_MONITORING_INFO = "monitoring_info"
    TOPIC_SERVER_MONITORING_FILTERED_INFO = "monitoring_filtered_data"
    MONITOR_GROUP_ID = "monitor_group"
    # -- db configuration
    _DB_CONFIGURATION_COMPONENT_GLOBAL = "GLOBAL"
    _DB_CONFIGURATION_COMPONENT_MONITOR = "MONITOR"

    DB_CONFIGURATION_SERVER = "SERVER"

    # ------ get values from configuration file ------
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

    # @staticmethod
    # def get_free_memory_threshold(logger=None):
    #     """free memory threshold, default 20 GB"""
    #     try:
    #         threshold = MonitorConst.__config.getint("monitor", "free_memory_threshold")
    #     except configparser.Error as ex:
    #         if logger:
    #             logger.warning("Getting configuration:(monitor, free_memory_threshold) failed:{0}. "
    #                            "Will use default value.".format(ex))
    #         threshold = 20
    #     return threshold
    #
    # @staticmethod
    # def get_free_disk_threshold(logger=None):
    #     """free disk threshold, default 50 GB"""
    #     try:
    #         threshold = MonitorConst.__config.getint("monitor", "free_disk_threshold")
    #     except configparser.Error as ex:
    #         if logger:
    #             logger.warning("Getting configuration:(monitor, free_disk_threshold) failed:{0}. "
    #                            "Will use default value.".format(ex))
    #         threshold = 50
    #     return threshold
    #
    # @staticmethod
    # def get_cpu_utilization_threshold(logger=None):
    #     """CPU utilization threshold, default 99(%) """
    #     try:
    #         threshold = MonitorConst.__config.getint("monitor", "cpu_utilization_threshold")
    #     except configparser.Error as ex:
    #         if logger:
    #             logger.warning("Getting configuration:(monitor, cpu_utilization_threshold) failed:{0}. "
    #                            "Will use default value.".format(ex))
    #         threshold = 99
    #     return threshold

    # @staticmethod
    # def get_check_interval(logger=None):
    #     try:
    #         interval = MonitorConst.__config.getint("monitor", "check_interval")
    #     except configparser.Error as ex:
    #         if logger:
    #             logger.warning("Getting configuration:(monitor, check_interval) failed:{0}. "
    #                            "Will use default value.".format(ex))
    #         interval = 1800
    #     return interval

    # @staticmethod
    # def get_max_failure_times(logger=None):
    #     try:
    #         max_failure_times = MonitorConst.__config.getint("monitor", "max_failure_times")
    #     except configparser.Error as ex:
    #         if logger:
    #             logger.warning("Getting configuration:(monitor, max_failure_times) failed:{0}. "
    #                            "Will use default value.".format(ex))
    #         max_failure_times = 3
    #     return max_failure_times

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
    logging.config.fileConfig(os.path.join(os.path.dirname(os.path.abspath(__file__)), "config/logging.ini"))
    __logger = logging.getLogger(MonitorConst.LOGGER_MONITOR_UTILITY)

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
    def is_current_time_working_time():
        current_time = datetime.now()
        hour = int(current_time.strftime('%H'))
        operation_hour_start, operation_hour_end = MonitorConst.get_operation_hours(MonitorUtility.__logger)
        weekday = current_time.weekday()
        return operation_hour_start <= hour <= operation_hour_end and weekday < 5

    @staticmethod
    def send_email(email_from,
                   email_to,
                   email_subject,
                   email_content,
                   email_cc=None,
                   email_password=None,
                   smtp_server="mail.sap.corp",
                   smtp_port=25):
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
            MonitorUtility.__logger.error("String is not accepted for the recipients! Please use list instead!")
            return
        # remove duplicates from email_to and email_cc
        email_to = list(set(email_to))
        email_cc = list(set(email_cc))
        server = None
        try:
            server = smtplib.SMTP(smtp_server, smtp_port)
            # server.connect('mail.sap.corp', 25)
            # server.ehlo()
            # server.starttls()
            # server.ehlo()
            if email_password is not None:
                server.login(email_from, email_password)

            email_body = "\r\n".join(["From: {0}".format(email_from),
                                      "To: {0}".format(",".join(email_to)),
                                      "CC: {0}".format(",".join(email_cc)),
                                      "Subject: {0}".format(email_subject),
                                      "",
                                      email_content])

            server.sendmail(email_from, email_to + email_cc, email_body)
            MonitorUtility.__logger.info("Email:{0} is sent to "
                                         "{1} successfully!".format(email_subject, email_to + email_cc))
        except Exception as ex:
            MonitorUtility.__logger.error("Failed to send email to "
                                          "{0}! Error:{1}, Subject:".format(email_to, ex, email_subject))
        finally:
            if server is not None:
                server.quit()

    @staticmethod
    def generate_email_body(server_info, email_type, additional_info=None):
        if email_type == MonitorConst.SERVER_INFO_MEM:
            total = round(server_info[MonitorConst.FIELD_MEM_TOTAL] / 1024 / 1024, 2)
            free = round(server_info[MonitorConst.FIELD_MEM_FREE] / 1024 / 1024, 2)
            check_type = "Memory"
        elif email_type == MonitorConst.SERVER_INFO_DISK:
            total = round(server_info[MonitorConst.FIELD_DISK_TOTAL] / 1024 / 1024, 2)
            free = round(server_info[MonitorConst.FIELD_DISK_FREE] / 1024 / 1024, 2)
            check_type = "Disk"
        else:  # MonitorConst.SERVER_INFO_CPU
            free = 0
            total = server_info[MonitorConst.FIELD_CPU_UTILIZATION]
            check_type = "CPU"

        server_name = server_info[MonitorConst.FIELD_SERVER_FULL_NAME]
        # memory_total = round(server_info["MEM_TOTAL"] / 1024 / 1024, 2)
        # memory_free = round(server_info["MEM_FREE"] / 1024 / 1024, 2)
        check_time = server_info[MonitorConst.FIELD_CHECK_TIME]

        body = ("Server Name:{0} \n"
                "\t CPU Utilization: {1}% \n"
                "\t Check Time:{2}".format(server_name, total, check_time)) \
            if email_type == MonitorConst.SERVER_INFO_CPU else (
                "Server Name:{0}\n"
                "\t Total {1}:{2}GB\n"
                "\t Free {3}:{4}GB\n"
                "\t Check Time:{5}".format(server_name, check_type, total, check_type, free, check_time))

        if additional_info:
            # top 5 consumers
            check_id = additional_info[0][MonitorConst.FIELD_CHECK_ID]
            body_additional = "\n Following is the top 5 {0} consumers, check id:{1}:\n ".format(check_type, check_id)
            unit_type = "GB" if email_type == MonitorConst.SERVER_INFO_DISK else "%"
            for consumer in additional_info:
                if email_type == MonitorConst.SERVER_INFO_DISK:
                    sid = consumer[MonitorConst.FIELD_FOLDER]
                    sid_or_folder = "Folder"
                else:
                    sid = MonitorUtility.get_sid_from_sidadm(consumer[MonitorConst.FIELD_USER_NAME])
                    sid_or_folder = "SID"
                body_additional = "".join([body_additional, ("\t {0}: {1}, Name: {2}, {3} Usage: {4}{5}"
                                                             "\n".format(sid_or_folder,
                                                                         sid,
                                                                         consumer[MonitorConst.FIELD_EMPLOYEE_NAME],
                                                                         check_type,
                                                                         consumer[MonitorConst.FIELD_USAGE],
                                                                         unit_type))])
            body = "".join([body, body_additional])
            # Will add more inform like: "This is the {0} warning email, the HANA instance which
            # consuming the most Memory will be shutdown after THREE warning email!!"
        return body

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
        # use <sid>adm, does not need this
        return "trextrex"
        # byte_to_decrypt = binascii.a2b_base64(string_to_decrypt)
        # with open(private_key_path, mode='rb') as private_file:
        #     key_data = private_file.read()
        #     private_data = rsa.PrivateKey.load_pkcs1(key_data)
        #     return rsa.decrypt(byte_to_decrypt, private_data).decode("utf-8")

    @staticmethod
    def log_debug(logger, message, location_id=None):
        if location_id is not None:
            message = "[{0}]{1}".format(location_id, message)
        if logger is not None:
            try:
                logger.debug(message)
            except Exception as ex:
                print("Logging [debug] message '{0}' failed, error:{1}".format(message, ex))
        else:
            print(message)

    @staticmethod
    def log_info(logger, message, location_id=None):
        if location_id is not None:
            message = "[{0}]{1}".format(location_id, message)
        if logger is not None:
            try:
                logger.info(message)
            except Exception as ex:
                print("Logging [info] message '{0}' failed, error:{1}".format(message, ex))
        else:
            print(message)

    @staticmethod
    def log_warning(logger, message, location_id=None):
        if location_id is not None:
            message = "[{0}]{1}".format(location_id, message)
        if logger is not None:
            try:
                logger.warning(message)
            except Exception as ex:
                print("Logging [warning] message '{0}' failed, error:{1}".format(message, ex))
        else:
            print(message)

    @staticmethod
    def log_error(logger, message, location_id=None):
        if location_id is not None:
            message = "[{0}]{1}".format(location_id, message)
        if logger is not None:
            try:
                logger.error(message)
            except Exception as ex:
                print("Logging [error] message '{0}' failed, error:{1}".format(message, ex))
        else:
            print(message)

    @staticmethod
    def log_exception(logger, message):
        if logger is not None:
            try:
                logger.exception(message)
            except Exception as ex:
                print("Logging [exception] message '{0}' failed, error:{1}".format(message, ex))
        else:
            print(message)
