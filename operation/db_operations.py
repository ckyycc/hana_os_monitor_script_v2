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

    def get_sid_mappings(self):
        query = "SELECT SID_START, SID_END,EMPLOYEE_ID FROM HANA_OS_MONITOR.SID_MAPPING_CFG "
        # "WHERE LOCATION_ID = {0}".format(location_id))
        return self.__query_select(query)

    def get_server_full_names(self, location_id):
        query = ("SELECT SERVER_ID, SERVER_FULL_NAME, MOUNT_POINT, OS FROM HANA_OS_MONITOR.SERVER_INFO "
                 "WHERE LOCATION_ID = {0} ORDER BY SERVER_ID".format(location_id))
        return self.__query_select(query)

    def get_current_process_status(self, location_id):
        query = ("SELECT TOP 1 STATUS FROM HANA_OS_MONITOR.M_MONITOR_CATALOG "
                 "WHERE STAGE = -1 AND LOCATION_ID = {0} ORDER BY CHECK_ID DESC".format(location_id))
        return self.__query_select(query)

    def get_current_servers_info(self, location_id):
        query = ("SELECT SERVER_ID, SERVER_FULL_NAME, DISK_TOTAL, DISK_FREE, MEM_TOTAL, MEM_FREE, "
                 "CPU_UTILIZATION, OS, CHECK_TIME, CHECK_ID FROM HANA_OS_MONITOR.M_CURRENT_SERVERS_INFO "
                 "WHERE LOCATION_ID ={0};".format(location_id))
        return self.__query_select(query)

    def get_top5_memory_consumers(self, server_id):
        query = ("SELECT SERVER_FULL_NAME, USER_NAME, EMPLOYEE_NAME, EMAIL, USAGE, "
                 "CHECK_ID, RANK FROM HANA_OS_MONITOR.M_TOP5_MEM_CONSUMERS WHERE SERVER_ID={0}".format(server_id))
        return self.__query_select(query)

    def get_top5_disk_consumers(self, server_id):
        query = ("SELECT SERVER_FULL_NAME, FOLDER, EMPLOYEE_NAME, EMAIL, USAGE, "
                 "CHECK_ID, RANK FROM HANA_OS_MONITOR.M_TOP5_DISK_CONSUMERS WHERE SERVER_ID={0}".format(server_id))
        return self.__query_select(query)

    def get_top5_cpu_consumers(self, server_id):
        query = ("SELECT SERVER_FULL_NAME, USER_NAME, EMPLOYEE_NAME, EMAIL, USAGE, "
                 "CHECK_ID, RANK FROM HANA_OS_MONITOR.M_TOP5_CPU_CONSUMERS WHERE SERVER_ID={0}".format(server_id))
        return self.__query_select(query)

    def get_last3_servers_info(self, server_id, threshold=None, op_type=None):
        """Get the last 3 server info by server id and threshold,
        if either threshold or op_type is None, only server id will be applied
        op_type can be MEM, CPU or Disk"""
        query = ("SELECT SERVER_FULL_NAME, DISK_TOTAL, DISK_FREE, MEM_TOTAL, MEM_FREE, CPU_UTILIZATION, "
                 "CHECK_ID FROM HANA_OS_MONITOR.M_LAST3_SERVERS_INFO WHERE SERVER_ID={0}".format(server_id))
        if threshold is not None and op_type is not None:
            if op_type == Mc.SERVER_INFO_MEM:
                query = "".join([query, " AND MEM_FREE < {0}".format(threshold)])
            elif op_type == Mc.SERVER_INFO_CPU:
                query = "".join([query, " AND CPU_UTILIZATION > {0}".format(threshold)])
            elif op_type == Mc.SERVER_INFO_DISK:
                query = "".join([query, " AND DISK_FREE < {0}".format(threshold)])

        return self.__query_select(query)

    def get_flag_of_sid(self, sid, server_id):
        query = "SELECT FILTER_FLAG FROM HANA_OS_MONITOR.SID_INFO WHERE SERVER_ID={0} AND SID='{1}'".format(server_id,
                                                                                                            sid)
        return self.__query_select(query)

    def get_failed_servers(self, check_id, location_id):
        """get the server which failed for all the 3 stages by location_id"""
        # query = ("SELECT B.SERVER_FULL_NAME FROM HANA_OS_MONITOR.M_MONITOR_CATALOG A "
        #          "INNER JOIN HANA_OS_MONITOR.SERVER_INFO B ON A.SERVER_ID = B.SERVER_ID "
        #          "WHERE A.STATUS = 'ERROR' AND A.CHECK_ID = '{0}' AND A.LOCATION_ID = {1} "
        #          "GROUP BY B.SERVER_FULL_NAME HAVING COUNT(1) >= 3".format(check_id, location_id))
        # commented on 2018/09/05 now only send the failing alert email at the first time after 8am of current day
        working_hour = Mc.get_operation_hours(self.__logger)[0]
        working_time = "0{0}".format(working_hour) if working_hour < 10 else "{0}".format(working_hour)
        query = ("SELECT SERVER_FULL_NAME FROM ( SELECT B.SERVER_FULL_NAME, A.SERVER_ID, A.CHECK_ID, COUNT(1) "
                 "AS FAILED_NUM FROM HANA_OS_MONITOR.M_MONITOR_CATALOG A "
                 "INNER JOIN HANA_OS_MONITOR.SERVER_INFO B ON A.SERVER_ID = B.SERVER_ID " 
                 "WHERE A.END_TIME >= TO_TIMESTAMP(TO_NVARCHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') "
                 "|| ' {0}:00:00', 'YYYY-MM-DD HH24:MI:SS') "
                 "AND A.STATUS = 'ERROR' AND A.CHECK_ID <= '{1}' AND A.LOCATION_ID = {2} "
                 "GROUP BY B.SERVER_FULL_NAME, A.SERVER_ID, A.CHECK_ID HAVING COUNT(1) >= 3 ) C "
                 "WHERE NOT EXISTS (SELECT 1 FROM HANA_OS_MONITOR.M_MONITOR_CATALOG D "
                 "WHERE D.CHECK_ID > C.CHECK_ID AND D.STATUS <> 'ERROR' AND D.SERVER_ID = C.SERVER_ID)"
                 "GROUP BY SERVER_FULL_NAME "
                 "HAVING SUM(FAILED_NUM) >= 3 AND SUM(FAILED_NUM) <6".format(working_time, check_id, location_id))

        return self.__query_select(query)

    def get_sudo_pwd_flag(self, server_id):
        query = "SELECT SUDO_PWD_FLAG FROM HANA_OS_MONITOR.SERVER_INFO WHERE SERVER_ID = {0}".format(server_id)
        return self.__query_select(query)

    def get_locations(self):
        query = "SELECT LOCATION_ID, LOCATION FROM HANA_OS_MONITOR.LOCATION_INFO"
        return self.__query_select(query)

    def get_email_admin(self, location_id):
        query = ("SELECT EMAIL FROM HANA_OS_MONITOR.EMPLOYEE_LOCATION_INFO A " 
                 "INNER JOIN HANA_OS_MONITOR.EMPLOYEE_INFO B ON A.EMPLOYEE_ID = B.EMPLOYEE_ID "
                 "WHERE A.LOCATION_ID = {0} AND "
                 "(UPPER(B.ADMIN) = 'X' OR UPPER(B.SUPER_ADMIN) = 'X')".format(location_id))
        return self.__query_select(query)

    def get_configuration(self, component, name):
        query = ("SELECT VALUE FROM HANA_OS_MONITOR.MONITOR_CONFIGURATION "
                 "WHERE CONFIGURATION = '{0}' AND COMPONENT = '{1}'".format(name, component))
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

    def update_monitor_catalog_overall_status(self, check_id, location_id):
        """Set the overall status for monitor_catalog,
        for one check_id, if status of sub steps (no WARNING status for sub steps) contains:
         1. 'COMPLETE' mix 'ERROR' --> 'WARNING'
         2. 'COMPLETE' --> 'WARNING'
         3. 'ERROR' --> 'ERROR'"""
        query = ("UPDATE HANA_OS_MONITOR.M_MONITOR_CATALOG SET END_TIME = CURRENT_TIMESTAMP, STATUS = "
                 "(SELECT 'WARNING' AS STATUS FROM DUMMY WHERE 'ERROR' IN "
                 "(SELECT STATUS FROM HANA_OS_MONITOR.M_MONITOR_CATALOG WHERE CHECK_ID = '{0}' AND LOCATION_ID = {1} "
                 "GROUP BY STATUS) AND 'COMPLETE' IN "
                 "(SELECT STATUS FROM HANA_OS_MONITOR.M_MONITOR_CATALOG WHERE CHECK_ID = '{0}' AND LOCATION_ID = {1} "
                 "GROUP BY STATUS) "
                 "UNION "
                 "SELECT 'ERROR' AS STATUS FROM DUMMY WHERE 'ERROR' IN "
                 "(SELECT STATUS FROM HANA_OS_MONITOR.M_MONITOR_CATALOG WHERE CHECK_ID = '{0}' AND LOCATION_ID = {1} "
                 "GROUP BY STATUS) AND EXISTS (SELECT COUNT(1) FROM ("
                 "SELECT STATUS FROM HANA_OS_MONITOR.M_MONITOR_CATALOG WHERE CHECK_ID = '{0}' AND LOCATION_ID = {1} "
                 "AND STAGE <> -1 GROUP BY STATUS) HAVING COUNT(1) = 1) "
                 "UNION "
                 "SELECT 'COMPLETE' AS STATUS FROM DUMMY WHERE 'COMPLETE' IN "
                 "(SELECT STATUS FROM HANA_OS_MONITOR.M_MONITOR_CATALOG WHERE CHECK_ID = '{0}' AND LOCATION_ID = {1} "
                 "GROUP BY STATUS) AND EXISTS (SELECT COUNT(1) FROM ("
                 "SELECT STATUS FROM HANA_OS_MONITOR.M_MONITOR_CATALOG WHERE CHECK_ID = '{0}' AND LOCATION_ID = {1} "
                 "AND STAGE <> -1 GROUP BY STATUS) HAVING COUNT(1) = 1)) "
                 "WHERE CHECK_ID = '{0}' AND LOCATION_ID = {1} AND STAGE = -1".format(check_id, location_id))
        self.__query_update(query)

    def update_monitor_catalog(self, check_id, server_id, stage, status, location_id, message):
        if status == Mc.MONITOR_STATUS_COMPLETE or \
                status == Mc.MONITOR_STATUS_ERROR or status == Mc.MONITOR_STATUS_WARNING:
            query = ("UPSERT HANA_OS_MONITOR.M_MONITOR_CATALOG "
                     "(CHECK_ID, SERVER_ID, LOCATION_ID, END_TIME, STATUS, STAGE, MESSAGE) "
                     "VALUES (?,?,?,CURRENT_TIMESTAMP,?,?,?) WITH PRIMARY KEY")
        else:
            query = ("UPSERT HANA_OS_MONITOR.M_MONITOR_CATALOG "
                     "(CHECK_ID, SERVER_ID, LOCATION_ID, END_TIME, STATUS, STAGE, MESSAGE) "
                     "VALUES (?,?,?,NULL,?,?,?) WITH PRIMARY KEY")

        param_list = [(check_id, server_id, location_id, status, stage, message)]
        self.__query_insert_batch(query, param_list)

    def update_server_monitoring_info(self, check_id, server_info, server_id):
        query = ("UPSERT HANA_OS_MONITOR.M_SERVER_INFO "
                 "(CHECK_ID,SERVER_ID,DISK_TOTAL,DISK_FREE,MEM_TOTAL,MEM_FREE,CPU_UTILIZATION) "
                 "VALUES (?,?,?,?,?,?,?) WITH PRIMARY KEY")
        param_list = [(check_id, server_id, server_info["DISK_TOTAL"], server_info["DISK_FREE"],
                       server_info["MEM_TOTAL"], server_info["MEM_FREE"], server_info["CPU_UTILIZATION"])]
        self.__query_insert_batch(query, param_list)

    def update_cpu_monitoring_info(self, check_id, server_id, top_5_cpu_consumers):
        query = ("INSERT INTO HANA_OS_MONITOR.M_CPU_INFO "
                 "(CHECK_ID,USER_NAME,SERVER_ID,PROCESS_ID,PROCESS_COMMAND,CPU) VALUES (?,?,?,?,?,?)")
        param_list = [(check_id, row["USER_NAME"], server_id, row["PROCESS_ID"], row["PROCESS_COMMAND"], row["CPU"])
                      for row in top_5_cpu_consumers]
        self.__query_insert_batch(query, param_list)

    def update_mem_monitoring_info(self, check_id, server_id, top_5_mem_consumers):
        query = ("INSERT INTO HANA_OS_MONITOR.M_MEM_INFO "
                 "(CHECK_ID,USER_NAME,SERVER_ID,PROCESS_ID,PROCESS_COMMAND,MEM) VALUES (?,?,?,?,?,?)")
        param_list = [(check_id, row["USER_NAME"], server_id, row["PROCESS_ID"], row["PROCESS_COMMAND"], row["MEM"])
                      for row in top_5_mem_consumers]
        self.__query_insert_batch(query, param_list)

    def update_disk_monitoring_info(self, check_id, server_id, disk_consuming_info):
        query = ("INSERT INTO HANA_OS_MONITOR.M_DISK_INFO "
                 "(CHECK_ID,SERVER_ID,FOLDER,USER_NAME,DISK_USAGE_KB) VALUES (?,?,?,?,?)")
        param_list = [(check_id, server_id, row["FOLDER"], row["USER_NAME"], row["DISK_USAGE_KB"])
                      for row in disk_consuming_info]
        self.__query_insert_batch(query, param_list)

    def update_instance_info(self, check_id, server_id, instance_info):
        query = ("INSERT INTO HANA_OS_MONITOR.M_INSTANCE_INFO"
                 "(CHECK_ID,SERVER_ID,SID,REVISION,INSTANCE_NUM,HOST,EDITION) VALUES (?,?,?,?,?,?,?)")
        param_list = [(check_id, server_id, r["SID"], r["REVISION"], r["INSTANCE_NUM"], r["HOST"], r["EDITION"])
                      for r in instance_info]

        self.__query_insert_batch(query, param_list)

    def insert_sid_info(self, sid_list):
        query = "INSERT INTO HANA_OS_MONITOR.SID_INFO (SERVER_ID, SID, SID_USER, EMPLOYEE_ID) VALUES (?,?,?,?)"
        self.__query_insert_batch(query, sid_list)

    def execute_from_script(self, sql_file):
        with open(sql_file, 'r') as sqL_file_handler:
            sql_scripts_file = sqL_file_handler.read()
            sql_commands = sql_scripts_file.strip().split(';')
            for command in sql_commands:
                if command:
                    self.__query_update(command)
