import threading

from util import MonitorUtility as Mu
from util import MonitorConst as Mc
from util import KafKaUtility as Ku
from util import InfoType

from errors import MonitorDBOpError
from operation.db_operations import HANAMonitorDAO


class DBOperator(threading.Thread):
    def __init__(self):
        super().__init__()
        self.__logger = Mu.get_logger(Mc.LOGGER_MONITOR_OPERATOR_DB)
        self._db_operator = HANAOperatorService.instance()

    def __operate(self, consumer):
        operators = {
            InfoType.MEMORY.value: self._db_operator.update_mem_monitoring_info,
            InfoType.CPU.value: self._db_operator.update_cpu_monitoring_info,
            InfoType.DISK.value: self._db_operator.update_disk_monitoring_info,
            InfoType.INSTANCE.value: self._db_operator.update_instance_monitoring_info
        }
        for msg in consumer:
            if msg and msg.value and Mc.MSG_TYPE in msg.value:
                operators[msg.value[Mc.MSG_TYPE]](msg.value)

    def run(self):
        """run the thread"""
        while True:
            consumer = Ku.get_consumer(Mc.MONITOR_GROUP_ID_DB_OPERATOR, Mc.TOPIC_FILTERED_INFO)
            self.__operate(consumer)
            Mu.log_warning(self.__logger, "Topic is empty or connection is lost. Trying to reconnect...")


class HANAOperatorService:
    """ HANA Server DB operator, responsible for all DB relative operations, it's designed as singleton.
    To get the instance of this class: HANAServerDBOperatorService.instance()
    Initialize the class using HANAServerDBOperatorService() will raise an exception.
    """
    __instance = None

    @staticmethod
    def instance():
        """static access method for singleton"""
        if HANAOperatorService.__instance is None:
            HANAOperatorService()
        return HANAOperatorService.__instance

    def __init__(self):
        # implement the singleton class
        if HANAOperatorService.__instance is not None:
            raise MonitorDBOpError("This class is a singleton, use HANAServerDBOperatorService.instance() instead")
        else:
            HANAOperatorService.__instance = self
            self.__monitor_dao = HANAMonitorDAO(Mc.get_hana_server(),
                                                Mc.get_hana_port(),
                                                Mc.get_hana_user(),
                                                Mc.get_hana_password())
            self.__logger = Mu.get_logger(Mc.LOGGER_MONITOR_OPERATOR_DB)

    def __update_server_info(self, check_id, server_id, **kwargs):
        disk_total = kwargs.get("disk_total", None)
        disk_free = kwargs.get("disk_free", None)
        mem_total = kwargs.get("mem_total", None)
        mem_free = kwargs.get("mem_free", None)
        cpu_usage = kwargs.get("cpu_usage", None)
        server_info = {Mc.FIELD_DISK_TOTAL: disk_total,
                       Mc.FIELD_DISK_FREE: disk_free,
                       Mc.FIELD_MEM_TOTAL: mem_total,
                       Mc.FIELD_MEM_FREE: mem_free,
                       Mc.FIELD_CPU_UTILIZATION: cpu_usage}
        self.__monitor_dao.update_server_monitoring_info(check_id, server_id, server_info)

    def update_mem_monitoring_info(self, info):
        cid = info[Mc.FIELD_CHECK_ID]
        sid = info[Mc.FIELD_SERVER_ID]
        self.__update_server_info(cid, sid, mem_total=info[Mc.FIELD_MEM_TOTAL], mem_free=info[Mc.FIELD_MEM_FREE])
        self.__monitor_dao.update_mem_monitoring_info(cid, sid, info[Mc.MSG_INFO])

    def update_disk_monitoring_info(self, info):
        cid = info[Mc.FIELD_CHECK_ID]
        sid = info[Mc.FIELD_SERVER_ID]
        self.__update_server_info(cid, sid, disk_total=info[Mc.FIELD_DISK_TOTAL], disk_free=info[Mc.FIELD_DISK_FREE])
        self.__monitor_dao.update_disk_monitoring_info(cid, sid, info[Mc.MSG_INFO])

    def update_cpu_monitoring_info(self, info):
        cid = info[Mc.FIELD_CHECK_ID]
        sid = info[Mc.FIELD_SERVER_ID]
        self.__update_server_info(cid, sid, cpu_usage=info[Mc.FIELD_CPU_UTILIZATION])
        self.__monitor_dao.update_cpu_monitoring_info(cid, sid, info[Mc.MSG_INFO])

    def update_instance_monitoring_info(self, info):
        cid = info[Mc.FIELD_CHECK_ID]
        sid = info[Mc.FIELD_SERVER_ID]
        self.__monitor_dao.update_instance_info(cid, sid, info[Mc.MSG_INFO])


if __name__ == '__main__':
    DBOperator().start()
