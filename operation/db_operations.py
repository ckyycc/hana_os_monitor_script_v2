from util import MonitorUtility as Mu
from util import MonitorConst as Mc
from errors import MonitorDBError
from hdbcli import dbapi

import traceback


class HANAMonitorDAO:
    """HANA Operator, response for operating database via the provided SQLs
    """

    def __init__(self, server_name, port, user, password):
        # get logging
        self.__logger = Mu.get_logger(Mc.LOGGER_HANA_DB)
        # get database connection
        try:
            Mu.log_info(self.__logger, "Connecting {0}:{1} with {2}".format(server_name, port, user))
            self.connection = dbapi.connect(server_name, port, user, password)
            if self.connection is None:
                Mu.log_error(self.__logger, "Connect to HANA error!")
            else:
                Mu.log_info(self.__logger, "Connected {0}:{1} with {2}".format(server_name, port, user))
        except Exception as ex:
            error_message = "Connect to HANA error:{0}".format(ex)
            Mu.log_error(self.__logger, error_message)
            Mu.log_exception(self.__logger, traceback.format_exc())
            raise MonitorDBError(error_message)

    def __del__(self):
        self.close_connection()

    def close_connection(self):
        if hasattr(self, "connection") and self.connection is not None:
            self.connection.close()
            self.connection = None
            Mu.log_debug(self.__logger, "Connecting is closed.")

    # ----------------------------Below is for select--------------------------------------

    def __query_select(self, query):
        cursor = None
        try:
            cursor = self.connection.cursor()
            self.__logger.debug("query:{0}".format(query))
            cursor.execute(query)
            ret = cursor.fetchall()
            Mu.log_debug(self.__logger, "Result record count {0}".format(len(ret)))
            return ret
        except Exception as ex:
            Mu.log_error(self.__logger, "Query:{0} failed with error:{1}".format(query, ex))
            Mu.log_exception(self.__logger, traceback.format_exc())
            return []
        finally:
            if cursor is not None:
                cursor.close()
            Mu.log_debug(self.__logger, "Cursor closed.")

    def get_server_full_names(self, location_id=None):
        query = ("SELECT SERVER_ID, SERVER_FULL_NAME, MOUNT_POINT, OS FROM HANA_OS_MONITOR.SERVER_INFO "
                 "WHERE LOCATION_ID = {0} ORDER BY SERVER_ID".format(location_id)) if location_id else \
            "SELECT SERVER_ID, SERVER_FULL_NAME, MOUNT_POINT, OS FROM HANA_OS_MONITOR.SERVER_INFO ORDER BY SERVER_ID"
        return self.__query_select(query)

    def get_info_by_5_sidadm_users(self, server_id, users):
        """ get all the info for the provides users,
        if can't find any users in the system, returns 1 record with server full name"""
        if users is None or len(users) == 0:
            return []

        # build user string
        users_sql = ""
        for user in users:  # len of users might be different
            users_sql = "SELECT '{0}' SID_USER FROM DUMMY".format(user) \
                if len(users_sql) == 0 else "{0} UNION ALL SELECT '{1}' SID_USER FROM DUMMY".format(users_sql, user)

        query = ("SELECT A.SERVER_ID, A.SERVER_FULL_NAME, IFNULL(E.SID, LEFT(UPPER(A.SID_USER),3)) SID, "
                 "A.SID_USER, E.FILTER_FLAG, E.EMPLOYEE_NAME, E.EMAIL "
                 "FROM ("
                 "SELECT SERVER_ID, SERVER_FULL_NAME, SID_USER FROM HANA_OS_MONITOR.SERVER_INFO , ({0}) "
                 "WHERE SERVER_ID = {1}) A "
                 "LEFT JOIN ( "
                 "SELECT B.SERVER_ID, B.SID, B.SID_USER, B.FILTER_FLAG, D.EMPLOYEE_NAME, D.EMAIL "
                 "FROM HANA_OS_MONITOR.SID_INFO B "
                 "INNER JOIN HANA_OS_MONITOR.EMPLOYEE_INFO D ON B.EMPLOYEE_ID = D.EMPLOYEE_ID "
                 "WHERE B.SERVER_ID = {1} "
                 ") E ON A.SID_USER = E.SID_USER".format(users_sql, server_id))

        return self.__query_select(query)

    def get_email_admin(self, server_id):
        query = ("SELECT EMAIL FROM HANA_OS_MONITOR.EMPLOYEE_LOCATION_INFO A "
                 "INNER JOIN HANA_OS_MONITOR.EMPLOYEE_INFO B ON A.EMPLOYEE_ID = B.EMPLOYEE_ID "
                 "INNER JOIN HANA_OS_MONITOR.SERVER_INFO C ON A.LOCATION_ID = C.LOCATION_ID "
                 "WHERE C.SERVER_ID = {0} AND "
                 "(UPPER(B.ADMIN) = 'X' OR UPPER(B.SUPER_ADMIN) = 'X')".format(server_id))
        return self.__query_select(query)

    def get_configuration(self, component=None, name=None):

        query = ("SELECT VALUE FROM HANA_OS_MONITOR.MONITOR_CONFIGURATION "
                 "WHERE CONFIGURATION = '{0}' AND COMPONENT = '{1}'".format(name, component)) \
            if component and name else "SELECT CONFIGURATION, VALUE FROM HANA_OS_MONITOR.MONITOR_CONFIGURATION"
        return self.__query_select(query)

    # ----------------------------Below is for update/insert/upsert--------------------------------------

    def __query_update(self, query, param=None):
        cursor = None
        try:
            cursor = self.connection.cursor()
            Mu.log_debug(self.__logger, "query:{0}, param:{1}".format(query, param))
            cursor.execute(query, param)
        except Exception as ex:
            Mu.log_error(self.__logger, "Query:{0} failed with error:{1}".format(query, ex))
            Mu.log_exception(self.__logger, traceback.format_exc())
        finally:
            if cursor is not None:
                cursor.close()
            Mu.log_debug(self.__logger, "Cursor closed.")

    def __query_insert_batch(self, query, param_list):
        cursor = None
        try:
            cursor = self.connection.cursor()
            Mu.log_debug(self.__logger, "query:{0}, param:{1}".format(query, param_list))
            cursor.executemany(query, param_list)
        except Exception as ex:
            Mu.log_error(self.__logger, "Query:{0} failed with error:{1}".format(query, ex))
            Mu.log_exception(self.__logger, traceback.format_exc())
        finally:
            if cursor is not None:
                cursor.close()
            Mu.log_debug(self.__logger, "Cursor closed.")

    def update_server_info(self, server_info, server_id):
        """update SERVER_INFO"""
        query = ("UPDATE HANA_OS_MONITOR.SERVER_INFO SET "
                 "DISK_TOTAL=:DISK_TOTAL, "
                 # "DISK_FREE=:DISK_FREE, "
                 "MEM_TOTAL=:MEM_TOTAL, "
                 # "MEM_FREE=:MEM_FREE, "
                 "CPU_NUMBER=:CPU_NUMBER, "
                 # "CPU_UTILIZATION=:CPU_UTILIZATION, "
                 "OS=:OS, "
                 "KERNEL=:KERNEL "
                 "WHERE SERVER_ID={0}""".format(server_id))
        self.__query_update(query, server_info)

    def update_server_monitoring_info(self, check_id, server_id, server_info):
        query = ("UPSERT HANA_OS_MONITOR.M_SERVER_INFO "
                 "(CHECK_ID,SERVER_ID,DISK_TOTAL,DISK_FREE,MEM_TOTAL,MEM_FREE,CPU_UTILIZATION) "
                 "VALUES (?,?,?,?,?,?,?) WITH PRIMARY KEY")
        param_list = [(check_id, server_id, server_info["DISK_TOTAL"], server_info["DISK_FREE"],
                       server_info["MEM_TOTAL"], server_info["MEM_FREE"], server_info["CPU_UTILIZATION"])]
        self.__query_insert_batch(query, param_list)

    def update_cpu_monitoring_info(self, check_id, server_id, cpu_info):
        query = ("INSERT INTO HANA_OS_MONITOR.M_CPU_INFO "
                 "(CHECK_ID,SERVER_ID,USER_NAME,CPU) VALUES (?,?,?,?)")
        # { user : usage }
        param_list = [(check_id, server_id, key, value) for key, value in cpu_info.items()]
        self.__query_insert_batch(query, param_list)

    def update_mem_monitoring_info(self, check_id, server_id, mem_info):
        query = "INSERT INTO HANA_OS_MONITOR.M_MEM_INFO (CHECK_ID,SERVER_ID,USER_NAME,MEM) VALUES (?,?,?,?)"
        # { user : usage }
        param_list = [(check_id, server_id, key, value) for key, value in mem_info.items()]
        self.__query_insert_batch(query, param_list)

    def update_disk_monitoring_info(self, check_id, server_id, disk_info):
        query = ("INSERT INTO HANA_OS_MONITOR.M_DISK_INFO "
                 "(CHECK_ID,SERVER_ID,FOLDER,USER_NAME,DISK_USAGE_KB) VALUES (?,?,?,?,?)")
        # { folder : { user : usage } }
        param_list = [(check_id, server_id, key, next(iter(value.keys())), next(iter(value.values())))
                      for key, value in disk_info.items()]
        self.__query_insert_batch(query, param_list)

    def update_instance_info(self, check_id, server_id, instance_info):
        query = ("INSERT INTO HANA_OS_MONITOR.M_INSTANCE_INFO"
                 "(CHECK_ID,SERVER_ID,SID,REVISION,INSTANCE_NUM,HOST,EDITION) VALUES (?,?,?,?,?,?,?)")
        param_list = [(check_id, server_id, r["SID"], r["REVISION"], r["INSTANCE_NUM"], r["HOST"], r["EDITION"])
                      for r in instance_info.values()]

        self.__query_insert_batch(query, param_list)

    def execute_from_script(self, sql_file):
        with open(sql_file, 'r') as sqL_file_handler:
            sql_scripts_file = sqL_file_handler.read()
            sql_commands = sql_scripts_file.strip().split(';')
            for command in sql_commands:
                if command:
                    self.__query_update(command)
