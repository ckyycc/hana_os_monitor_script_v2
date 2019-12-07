from config_monitor import ConfigMonitor
from coordinator import MonitorCoordinator
from analyzer import DataAnalyzer
from db_operator import DBOperator
from alarm_operator import AlarmOperator
from app_operator import AppOperator


class Monitor:
    """The main Class for the Monitor Tool
        start_monitor_system  --  Start the monitor tool
    """

    @staticmethod
    def start_monitor_system():
        """
        Start the whole monitor system. Actually all the parts of the monitor system can be run via python file alone
        """
        # start config monitor
        ConfigMonitor().start()
        # start coordinator
        MonitorCoordinator().start()
        # start analyzer
        DataAnalyzer().start()
        # start db operator
        DBOperator().start()
        # start alarm operator
        AlarmOperator().start()
        # start application operator
        AppOperator().start()


if __name__ == '__main__':
    Monitor.start_monitor_system()

