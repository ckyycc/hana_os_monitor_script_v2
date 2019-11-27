from config_monitor import ConfigMonitor
from coordinator import MonitorCoordinator
from analyzer import DataAnalyzer
from db_operator import DBOperator


class Monitor:
    """The main Class for the Monitor Tool
        start_monitor_system  --  Start the monitor tool
    """

    @staticmethod
    def start_monitor_system():
        # start config monitor
        ConfigMonitor().start()
        # start coordinator
        MonitorCoordinator().start()
        # start analyzer
        DataAnalyzer().start()
        # start db operator
        DBOperator().start()


if __name__ == '__main__':
    Monitor.start_monitor_system()

