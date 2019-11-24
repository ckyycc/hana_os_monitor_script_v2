from config_monitor import ConfigMonitor
from coordinator import MonitorCoordinator


class Monitor:
    """The root (abstract) Class for the Monitor Tools
        accept      --  Accept the updater to perform the DB operations
        monitoring  --  performing the monitoring job for memory, CPU and Disk
    """

    def __init__(self):
        pass

    @staticmethod
    def start_monitor_system():
        # start config monitor
        ConfigMonitor().start()
        # start coordinator
        MonitorCoordinator().start()


if __name__ == '__main__':
    Monitor.start_monitor_system()

# class HANAServerDBOperatorService:
#     """ HANA Server DB operator, responsible for all DB relative operations, it's designed as singleton.
#     To get the instance of this class: HANAServerDBOperatorService.instance()
#     Initialize the class using HANAServerDBOperatorService() will raise an exception.
#     """
#     __instance = None
#
#     @staticmethod
#     def instance():
#         """static access method for singleton"""
#         if HANAServerDBOperatorService.__instance is None:
#             HANAServerDBOperatorService()
#         return HANAServerDBOperatorService.__instance
#
#     def __init__(self):
#         # implement the singleton class
#         if HANAServerDBOperatorService.__instance is not None:
#             raise MonitorDBOpError("This class is a singleton, use HANAServerDBOperatorService.instance() instead")
#         else:
#             HANAServerDBOperatorService.__instance = self
#             self.__monitor_dao = HANAMonitorDAO(Mc.get_hana_server(),
#                                                 Mc.get_hana_port(),
#                                                 Mc.get_hana_user(),
#                                                 Mc.get_hana_password())
#             self.__logger = Mu.get_logger(Mc.LOGGER_MONITOR_SERVER_DB_OPERATOR)
#
#     def get_configurations(self):
#         db_output_configs = self.__monitor_dao.get_configuration()
#         db_output_servers = self.__monitor_dao.get_server_full_names()
#         try:
#             configs = {cfg[0]: int(cfg[1]) if cfg[0].endswith("_INT") else cfg[1] for cfg in db_output_configs}
#             configs["servers"] = [{Mc.FIELD_SERVER_ID: server[0],
#                                   Mc.FIELD_SERVER_FULL_NAME: server[1],
#                                   Mc.FIELD_MOUNT_POINT: server[2],
#                                   Mc.FIELD_OS: server[3]} for server in db_output_servers]
#
#         except Exception as ex:
#             configs = {}
#             Mu.log_warning(self.__logger, "Parsing DB output failed in 'retrieve_configurations' "
#                                           "with error: {0}, the output: {1}, {2}".format(ex,
#                                                                                          db_output_configs,
#                                                                                          db_output_servers))
#         return configs
