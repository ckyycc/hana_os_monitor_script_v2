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
        print("Starting ConfigMonitor")
        ConfigMonitor().start()
        # start coordinator
        print("Starting Coordinator")
        MonitorCoordinator().start()
        # start analyzer
        print("Starting DataAnalyzer")
        DataAnalyzer().start()
        # start db operator
        print("Starting DBOperator")
        DBOperator().start()
        # start alarm operator
        print("Starting AlarmOperator")
        AlarmOperator().start()
        # start application operator
        print("Starting AppOperator")
        AppOperator().start()
        print("Done.")


if __name__ == '__main__':
    Monitor.start_monitor_system()

