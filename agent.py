import getopt
import sys
import os
import signal
import getpass
import subprocess
from util import MonitorUtility as Mu
from util import MonitorConst as Mc
from util import KafKaUtility as Ku
from util import InfoType
from errors import MonitorDBOpError
from errors import MonitorOSOpError
from abc import ABC, abstractmethod
from threading import Thread
import time
import re
import atexit


class Monitor(ABC, Thread):
    """The root (abstract) Class for the Monitor Tools
        accept      --  Accept the updater to perform the DB operations
        monitoring  --  performing the monitoring job for memory, CPU and Disk
    """

    def __init__(self):
        super().__init__()
        self._os_operator = HANAServerOSOperatorService.instance()
        self._msg_producer = MsgProducerService.instance()
        self._server_name = self._os_operator.get_host_name()

    def accept(self, dispatcher):
        """Accept the visitors to perform the DB operations"""
        dispatcher.dispatch(self)

    def get_msg_producer(self):
        return self._msg_producer

    @abstractmethod
    def monitoring(self, check_id):
        """abstract method, needs to be overwritten in child classes"""
        pass

    @abstractmethod
    def _get_interval(self):
        """abstract method, needs to be overwritten in child classes"""
        pass

    def run(self):
        """run the thread"""
        while True:
            check_id = Mu.generate_check_id()
            self.monitoring(check_id)
            time.sleep(self._get_interval())


class MemoryMonitor(Monitor):
    """Monitoring for Memory, get all the detail of memory consumption for all HANA related users"""

    def __init__(self, server_id, interval):
        super().__init__()
        self.__logger = Mu.get_logger(Mc.LOGGER_MONITOR_MEM)
        self.__server_id = server_id
        self.__interval = interval

    def monitoring(self, check_id):
        try:
            server_id = self.__server_id
            server_name = self._server_name

            Mu.log_debug(self.__logger, "[{0}]Memory Monitoring begin...".format(check_id))
            Mu.log_debug(self.__logger, "Trying to get memory overview of {0}".format(server_name))

            # collect memory info: total memory and free memory
            mem_total, mem_free = self._os_operator.collect_mem_info(server_name)
            Mu.log_debug(self.__logger,
                         "Memory overview of {0} is (total:{1}, free:{2})".format(server_name, mem_total, mem_free))

            mem_info = self._os_operator.get_mem_consumers(server_name)
            Mu.log_debug(self.__logger,
                         "memory consuming information for {0}:{1}".format(server_name, mem_info))

            # insert overview to memory info
            mem_info.insert(0, {Mc.FIELD_CHECK_ID: check_id,
                                Mc.FIELD_MEM_TOTAL: mem_total,
                                Mc.FIELD_MEM_FREE: mem_free})

            # send the info
            self.accept(MonitorResourceDispatcher(mem_info, server_id))

            Mu.log_info(self.__logger, "Memory Monitoring is done for {0}.".format(server_name))
        except Exception as ex:
            Mu.log_warning_exc(self.__logger, "Error Occurred when performing Memory monitoring, ERROR: {0}".format(ex))

    def _get_interval(self):
        return self.__interval if self.__interval is not None else 60


class CPUMonitor(Monitor):
    """Monitoring for CPU, get top 5 CPU consumers that relative to HANA by user from all the servers"""

    def __init__(self, server_id, interval):
        super().__init__()
        self.__logger = Mu.get_logger(Mc.LOGGER_MONITOR_CPU)
        self.__server_id = server_id
        self.__interval = interval

    def monitoring(self, check_id):
        try:
            server_id = self.__server_id
            server_name = self._server_name

            Mu.log_debug(self.__logger, "[{0}]CPU Monitoring begin...".format(check_id))
            Mu.log_debug(self.__logger, "Trying to get CPU overview of {0}".format(server_name))
            # collect cpu info: cpu_num, cpu_usage
            cpu_num, cpu_usage = self._os_operator.collect_cpu_info(server_name)

            Mu.log_debug(self.__logger,
                         "CPU overview of {0} is (num:{1}, usage:{2})".format(server_name, cpu_num, cpu_usage))

            cpu_info = self._os_operator.get_cpu_consumers(server_name)
            Mu.log_debug(self.__logger, "CPU consuming information for {0}:{1}".format(server_name, cpu_info))

            # insert overview to cpu info
            cpu_info.insert(0, {Mc.FIELD_CHECK_ID: check_id,
                                Mc.FIELD_CPU_NUMBER: cpu_num,
                                Mc.FIELD_CPU_UTILIZATION: cpu_usage})

            self.accept(MonitorResourceDispatcher(cpu_info, server_id))

            Mu.log_info(self.__logger, "CPU Monitoring is done for {0}.".format(server_name))
        except Exception as ex:
            Mu.log_warning_exc(self.__logger, "Error Occurred when performing CPU monitoring, ERROR: {0}".format(ex))

    def _get_interval(self):
        return self.__interval if self.__interval is not None else 60


class DiskMonitor(Monitor):
    """Monitoring for Disk, get disk consuming information by user from all the servers"""

    def __init__(self, server_id, mount_point, interval):
        super().__init__()
        self.__logger = Mu.get_logger(Mc.LOGGER_MONITOR_DISK)
        self.__server_id = server_id
        self.__mount_point = mount_point
        self.__interval = interval

    def monitoring(self, check_id):
        try:
            server_id = self.__server_id
            server_name = self._server_name
            mount_point = self.__mount_point

            Mu.log_debug(self.__logger, "[{0}]Disk Monitoring begin...".format(check_id))
            Mu.log_debug(self.__logger, "Trying to get disk overview of {0}".format(server_name))
            # collect disk overview info: disk_total, disk_free
            disk_total, disk_free = self._os_operator.collect_disk_info(server_name, mount_point)
            Mu.log_debug(self.__logger,
                         "Disk overview of {0} is (total:{1}, free:{2})".format(server_name, disk_total, disk_free))

            disk_info = self._os_operator.get_disk_consumers(server_name, mount_point)

            Mu.log_debug(self.__logger, "Disk consuming information for {0}:{1}".format(server_name, disk_info))

            # insert overview to memory info
            disk_info.insert(0, {Mc.FIELD_CHECK_ID: check_id,
                                 Mc.FIELD_DISK_TOTAL: disk_total,
                                 Mc.FIELD_DISK_FREE: disk_free})

            self.accept(MonitorResourceDispatcher(disk_info, server_id))
            Mu.log_info(self.__logger, "Disk Monitoring is done for {0}.".format(server_name))
        except Exception as ex:
            Mu.log_warning_exc(self.__logger, "Error Occurred when performing Disk monitoring, ERROR: {0}".format(ex))

    def _get_interval(self):
        return self.__interval if self.__interval is not None else 3600


class InstanceInfoMonitor(Monitor):
    """Collecting basic info for hana instances, update hana instance info from all the servers.
    Including SID, instance number, host, server name, revision, edition and so on.
    Not to affect the overall status calculation, this stage is not counted when calculating status of overall stage
    """
    def __init__(self, server_id, interval):
        super().__init__()
        self.__logger = Mu.get_logger(Mc.LOGGER_MONITOR_INSTANCE)
        self.__server_id = server_id
        self.__interval = interval

    def monitoring(self, check_id):
        try:
            server_id = self.__server_id
            server_name = self._server_name
            Mu.log_debug(self.__logger, "[{0}]Instance Monitoring begin...".format(check_id))
            Mu.log_debug(self.__logger, "Trying to get instance info of {0}".format(server_name))
            # collect instance info for one server by server id
            instance_info = self._os_operator.get_all_hana_instance_info(server_id)
            Mu.log_debug(self.__logger, "Instance information of {0} is {1}".format(server_name, instance_info))

            if instance_info:  # will skip sending instance info if it is empty
                instance_info.insert(0, {Mc.FIELD_CHECK_ID: check_id})
                self.accept(MonitorResourceDispatcher(instance_info, server_id))
            else:
                Mu.log_debug(self.__logger,
                             "Instance information for {0} is empty, skipped sending this info.".format(server_name))
            Mu.log_info(self.__logger, "Instance Monitoring is done for {0}.".format(server_name))
        except Exception as ex:
            Mu.log_warning_exc(self.__logger, "Error Occurred when monitoring Instance, ERROR: {0}".format(ex))

    def _get_interval(self):
        return self.__interval if self.__interval is not None else 3600


class ResourceInfoDispatcher(ABC):
    @abstractmethod
    def dispatch(self, monitor):
        """abstract method, needs to be overwritten in child classes"""
        pass


class MonitorResourceDispatcher(ResourceInfoDispatcher):
    """Visitor for all monitor classes, it's for updating information of the monitoring processes including
    the monitoring for the memory, CPU and disk to HANA DB"""

    def __init__(self, monitor_info, server_id):
        self.__monitor_info = monitor_info
        self.__server_id = server_id

    def dispatch(self, monitor):
        if isinstance(monitor, MemoryMonitor):
            self.__dispatch_memory_info(monitor)
        elif isinstance(monitor, CPUMonitor):
            self.__dispatch_cpu_info(monitor)
        elif isinstance(monitor, DiskMonitor):
            self.__dispatch_disk_info(monitor)
        elif isinstance(monitor, InstanceInfoMonitor):
            self.__dispatch_instance_info(monitor)

    def __dispatch_memory_info(self, monitor):
        monitor.get_msg_producer().delivery(self.__monitor_info, InfoType.MEMORY.value, self.__server_id)

    def __dispatch_cpu_info(self, monitor):
        monitor.get_msg_producer().delivery(self.__monitor_info, InfoType.CPU.value, self.__server_id)

    def __dispatch_disk_info(self, monitor):
        monitor.get_msg_producer().delivery(self.__monitor_info, InfoType.DISK.value, self.__server_id)

    def __dispatch_instance_info(self, monitor):
        monitor.get_msg_producer().delivery(self.__monitor_info, InfoType.INSTANCE.value, self.__server_id)


class HANAServerOSOperatorService:
    """ Server OS side operator, responsible for all shell command operations, it's designed as singleton.
    To get the instance of this class: HANAServerOSOperatorService.instance()
    Initialize the class using HANAServerOSOperatorService() will raise an exception.
    """
    __instance = None

    @staticmethod
    def instance():
        """static access method for singleton"""
        if HANAServerOSOperatorService.__instance is None:
            HANAServerOSOperatorService()
        return HANAServerOSOperatorService.__instance

    def __init__(self):
        if HANAServerOSOperatorService.__instance is not None:
            raise MonitorOSOpError("This class is a singleton, use HANAServerOSOperatorService.instance() instead")
        else:
            HANAServerOSOperatorService.__instance = self
            self.__logger = Mu.get_logger(Mc.LOGGER_MONITOR_SERVER_OS_OPERATOR)

    def get_host_name(self):
        try:
            host_name = os.uname()[1]
        except Exception as ex:
            Mu.log_warning(self.__logger, "Getting host name for server failed with error:{0}, .".format(ex))
            host_name = ""
        return host_name

    def collect_disk_info(self, server_name, mount_point):
        """collect disk info, including total size and unused size"""
        os_output = HANAServerOSOperatorService.__exec(
            "df -l| grep .*'\s'{0}$ | {1}".format(mount_point, "awk '{print $(NF-4) \" \" $(NF-2)}'"))
        if os_output is None:
            Mu.log_warning(self.__logger, "Can not get disk info for server:{0}, "
                                          "mount_point:{1}.".format(server_name, mount_point))
            total_size = -1
            unused_size = -1
        else:
            try:
                results = os_output[0].split()
                total_size = float(results[0])
                unused_size = float(results[1])
            except Exception as ex:
                total_size = -1
                unused_size = -1
                Mu.log_warning(self.__logger, "Parsing output failed in 'collect_disk_info' with error: {0}, "
                                              "server: {1}, the output: {2}".format(ex, server_name, os_output))
        return total_size, unused_size

    def collect_mem_info(self, server_name):
        """ get the overall memory information for system"""
        os_output = HANAServerOSOperatorService.__exec("free | tail -3 | xargs | awk '{print $2 \" \"  $11}'")

        if not os_output:
            Mu.log_warning(self.__logger, "Can not get memory info for server:{0}.".format(server_name))
            mem_total = -1
            mem_free = -1
        else:
            try:
                results = os_output[0].split()
                mem_total = int(results[0])
                mem_free = int(results[1])
            except Exception as ex:
                mem_total = -1
                mem_free = -1
                Mu.log_warning(self.__logger, "Parsing output failed in 'collect_mem_info' with error: {0}, "
                                              "server: {1}, the output: {2}".format(ex, server_name, os_output))

        return mem_total, mem_free

    def collect_cpu_info(self, server_name):
        """ get the overall CPU information for system"""

        # get CPU Number
        os_output_cpu_number = HANAServerOSOperatorService.__exec("getconf _NPROCESSORS_ONLN")

        # get CPU Usage
        # Below modification is for the CPU Usage empty (-1%) bug:
        # for some user in some server, the mpstat output is:
        # Linux 3.0.101-65-default (vanpghana04) 07/16/18 _x86_64_,
        #
        # 23:21:36     CPU    %usr   %nice    %sys %iowait    %irq   %soft  %steal  %guest   %idle,
        # 23:21:37     all    0.33    0.00    0.26    0.00    0.00    0.00    0.00    0.00   99.41,
        # Average:     all    0.33    0.00    0.26    0.00    0.00    0.00    0.00    0.00   99.41
        # While, for some user in some server the output is
        # Linux 3.0.101-65-default (vanpghana04) 07/16/18 _x86_64_,
        #
        # 11:22:44 PM  CPU    %usr   %nice    %sys %iowait    %irq   %soft  %steal  %guest   %idle,
        # 11:22:45 PM  all    0.44    0.00    0.29    0.00    0.00    0.00    0.00    0.00   99.27,
        # Average:     all    0.44    0.00    0.29    0.00    0.00    0.00    0.00    0.00   99.27

        # so below command will not get the result when there is no column 3 = CPU($3 != CPU)
        #     "mpstat 1 1 | awk '$3 ~ /CPU/ { for(i=1;i<=NF;i++) { if ($i ~ /%idle/) field=i }} $3 ~ /all/ "
        #     "{ print 100 - $field }'", ssh)
        # Below is not good, will be failed if last column is not idle
        # cmd_output = self.__ssh_exec_command("mpstat 1 1 | awk 'NR==4 {print 100 - $NF}'", ssh)
        # Below is better, but still need to use hard code(select the forth line: NR==4)
        #     "mpstat 1 1 | awk '{for(i=1;i<=NF;i++) {if ($i ~ /%idle/) field=i}} NR==4 {print 100 - $field}'", ssh)
        # Below is the best, select line witch contains '/all/'
        os_output_cpu_usage = HANAServerOSOperatorService.__exec(
            "mpstat 1 1 | awk '{for(i=1;i<=NF;i++) {if ($i ~ /%idle/) field=i}} /all/ {print 100 - $field; exit}'")

        # get cpu number
        if os_output_cpu_number is None:
            Mu.log_warning(self.__logger, "Can not get cpu number info for server:{0}.".format(server_name))
            cpu_number = -1
        else:
            try:
                cpu_number = int(os_output_cpu_number[0])
            except Exception as ex:
                cpu_number = -1
                Mu.log_warning(self.__logger, "Parsing output failed in 'collect_cpu_info(0)' "
                                              "with error: {0}, server: {1}, "
                                              "the output: {2}".format(ex, server_name, os_output_cpu_number))
        # get cpu usage
        if os_output_cpu_usage is None:
            Mu.log_warning(self.__logger, "Can not get cpu usage info for server:{0}.".format(server_name))
            cpu_usage = -1
        else:
            try:
                cpu_usage = float(os_output_cpu_usage[0])
            except Exception as ex:
                cpu_usage = -1
                Mu.log_warning(self.__logger, "Parsing output failed in 'collect_cpu_info(1)' "
                                              "with error: {0}, server: {1}, "
                                              "the output: {2}".format(ex, server_name, os_output_cpu_usage))

        return cpu_number, cpu_usage

    def get_mem_consumers(self, server_name):
        """Get memory consumers for all users, contains top 100 memory consumers for every user """
        os_output = HANAServerOSOperatorService.__exec(
            "cut -d: -f1 /etc/passwd | grep ^[A-Za-z0-9][A-Za-z0-9][A-Za-z0-9][a]dm$ "
            "| awk '{for (i=1;i<=NF;i++) {system(\"ps -Ao user,comm,pid,pmem | "
            "grep \"$i\"  | sort -k 4 -nr | head -100\")}}'")

        if os_output is None:
            Mu.log_warning(self.__logger, "Can not get memory consumers of ({0}).".format(server_name))
            mem_consumers = []
        else:
            try:
                # replace ' <defunct>' to '<defunct>' to prevent from using <defunct> as process id
                mem_consumers = [i.replace(' <defunct>', '<defunct>').split() for i in os_output]
                mem_consumers = [{Mc.FIELD_USER_NAME: i[0],
                                  Mc.FIELD_PROCESS_COMMAND: i[1],
                                  Mc.FIELD_PROCESS_ID: i[2],
                                  Mc.FIELD_MEM: i[3]} for i in mem_consumers if len(i) > 3]

            except Exception as ex:
                mem_consumers = []
                Mu.log_warning(self.__logger, "Parsing output failed in 'get_mem_consumers' with error:{0}, "
                                              "server:{1}, the output:{2}".format(ex, server_name, os_output))

        # mem_consumers = []
        # user_name =
        # ["ck0adm", "ck1adm", "ck2adm", "ck3adm", "ck4adm", "ck5adm", "ck6adm", "ck7adm", "ck8adm", "ck9adm",
        # "yy0adm", "yy1adm", "yy2adm", "yy3adm", "yy4adm", "yy5adm", "yy6adm", "yy7adm", "yy8adm", "yy9adm",
        # "cc0adm", "cc1adm", "cc2adm", "cc3adm", "cc4adm", "cc5adm", "cc6adm", "cc7adm", "cc8adm", "cc9adm",
        # "cy0adm", "cy1adm", "cy2adm", "cy3adm", "cy4adm", "cy5adm", "cy6adm", "cy7adm", "cy8adm", "cy9adm"]
        # for x in range(1000):
        #     mem_consumers.append({Mc.FIELD_USER_NAME: user_name[random.randint(0, 29)],
        #                           Mc.FIELD_PROCESS_COMMAND: 'test_cmd{0}'.format(random.randint(0, 1000)),
        #                           Mc.FIELD_PROCESS_ID: random.randint(1000, 2000),
        #                           Mc.FIELD_MEM: random.randint(0, 100)})

        return mem_consumers

    def get_cpu_consumers(self, server_name):
        """Get cpu consumers for all users, contains top 5 cpu consumers for every user """
        os_output = HANAServerOSOperatorService.__exec(
            "cut -d: -f1 /etc/passwd | grep ^[A-Za-z0-9][A-Za-z0-9][A-Za-z0-9][a]dm$ "
            "| awk '{for (i=1;i<=NF;i++) {system(\"top -u \"$i\" -bn1 | sed -n '8,12'p\")}}'")

        if os_output is None:
            Mu.log_warning(self.__logger, "Can not get cpu consumers for ({0}).".format(server_name))
            cpu_consumers = []
        else:
            try:
                cpu_consumers = [{Mc.FIELD_USER_NAME: i.split()[1],
                                  Mc.FIELD_PROCESS_COMMAND: i.split()[11],
                                  Mc.FIELD_PROCESS_ID: i.split()[0],
                                  Mc.FIELD_CPU: i.split()[8]} for i in os_output if len(i.split()) > 11]

                # In some system, some user might have the same user id (by the wrong setting),
                # which lead to the duplicate key issue.
                # remove duplicated records (list(set()) will not work, because CPU utilization may not the same)
                cpu_consumers_clean = []
                for consumer in cpu_consumers:
                    duplicate_flag = False
                    for consumer_clean in cpu_consumers_clean:
                        if consumer[Mc.FIELD_USER_NAME] == consumer_clean[Mc.FIELD_USER_NAME] and \
                                consumer[Mc.FIELD_PROCESS_COMMAND] == consumer_clean[Mc.FIELD_PROCESS_COMMAND] and \
                                consumer[Mc.FIELD_PROCESS_ID] == consumer_clean[Mc.FIELD_PROCESS_ID]:
                            duplicate_flag = True
                            break
                    if duplicate_flag:
                        continue
                    else:
                        cpu_consumers_clean.append(consumer)

                cpu_consumers = cpu_consumers_clean
            except Exception as ex:
                cpu_consumers = []
                Mu.log_warning(self.__logger, "Parsing output failed in 'get_cpu_consumers' with error:{0}, "
                                              "server:{1}, the output:{2}".format(ex, server_name, os_output))

        # cpu_consumers = []
        # user_name =
        # ["ck0adm", "ck1adm", "ck2adm", "ck3adm", "ck4adm", "ck5adm", "ck6adm", "ck7adm", "ck8adm", "ck9adm",
        # "yy0adm", "yy1adm", "yy2adm", "yy3adm", "yy4adm", "yy5adm", "yy6adm", "yy7adm", "yy8adm", "yy9adm",
        # "cc0adm", "cc1adm", "cc2adm", "cc3adm", "cc4adm", "cc5adm", "cc6adm", "cc7adm", "cc8adm", "cc9adm",
        # "cy0adm", "cy1adm", "cy2adm", "cy3adm", "cy4adm", "cy5adm", "cy6adm", "cy7adm", "cy8adm", "cy9adm"]
        # for x in range(1000):
        #     cpu_consumers.append({Mc.FIELD_USER_NAME: user_name[random.randint(0, 29)],
        #                           Mc.FIELD_PROCESS_COMMAND: 'test_cmd{0}'.format(random.randint(0, 1000)),
        #                           Mc.FIELD_PROCESS_ID: random.randint(1000, 2000),
        #                           Mc.FIELD_CPU: random.randint(0, 100)})
        return cpu_consumers

    def get_disk_consumers(self, server_name, mount_point):
        """ Get the disk consuming information for mount_point (default value is /usr/sap)"""
        # exclude mount_point/tmp: --exclude=/usr/sap/tmp. It's for the hanging issue of llbpal97,
        # modified at 2018/07/22
        # exclude mount_point/shared: --exclude=/usr/sap/shared. It's for the hanging issue of vanpghana06 and 07
        # modified at 2018/07/26
        # exclude mount_point/temp. It's for hanging issue of llbpal96
        # modified at 2019/07/05
        os_output = HANAServerOSOperatorService.__exec(
            "du --exclude={0}/tmp --exclude={0}/temp --exclude={0}/shared --exclude=/usr/sap/eua_paths "
            "--max-depth=1 {0} 2>>/dev/null".format(mount_point))

        os_output_owners = []
        if os_output is None:
            Mu.log_warning(self.__logger, "Can not get disk consumers for "
                                          "({0}:{1}).".format(server_name, mount_point))
            disk_usage_info = []
        else:
            try:
                # get owner of the all folders in mount_point
                os_output_owners = HANAServerOSOperatorService.__exec(
                    "".join(["ls -ld {0}/* | awk ".format(mount_point), "'{print $3\"\t\"$NF}'"]))

                # for filter purpose, add "/" at the end of mount_point
                mount_point = "".join([mount_point, "/"])

                disk_usage_info = [{Mc.FIELD_DISK_USAGE_KB: int(i.split()[0]),
                                    Mc.FIELD_FOLDER: i.split()[1]
                                    [i.split()[1].startswith(mount_point) and len(mount_point):],
                                    Mc.FIELD_USER_NAME: next(
                                        (j.split()[0] for j in os_output_owners
                                         if len(j.split()) == 2 and i.split()[1] == j.split()[1]), '')}
                                   for i in os_output if len(i.split()) == 2 and mount_point in i.split()[1]]

            except Exception as ex:
                disk_usage_info = []
                Mu.log_warning(self.__logger, "Parsing SSH output failed in 'get_disk_consumers' with error: {0}, "
                                              "server: {1}, the output: {2}, "
                                              "owners: {3}".format(ex, server_name, os_output, os_output_owners))

        # disk_usage_info = []
        # user_name =
        # ["ck0adm", "ck1adm", "ck2adm", "ck3adm", "ck4adm", "ck5adm", "ck6adm", "ck7adm", "ck8adm", "ck9adm",
        # "yy0adm", "yy1adm", "yy2adm", "yy3adm", "yy4adm", "yy5adm", "yy6adm", "yy7adm", "yy8adm", "yy9adm",
        # "cc0adm", "cc1adm", "cc2adm", "cc3adm", "cc4adm", "cc5adm", "cc6adm", "cc7adm", "cc8adm", "cc9adm",
        # "cy0adm", "cy1adm", "cy2adm", "cy3adm", "cy4adm", "cy5adm", "cy6adm", "cy7adm", "cy8adm", "cy9adm"]
        # for x in range(3):
        #     disk_usage_info.append({Mc.FIELD_USER_NAME: user_name[random.randint(0, 29)],
        #                             Mc.FIELD_FOLDER: 'testFolder{0}'.format(random.randint(0, 1000)),
        #                             Mc.FIELD_DISK_USAGE_KB: random.randint(10000000, 1000000000)})
        return disk_usage_info

    def __get_all_hana_instance_info(self, path):
        # switch to hdblcm because it contains nodes num info and will not hang when scanning some share folder
        # but needs to copy hdblcm to all home/bin path or use some share folder
        if path is None:
            return self.__exec("~/bin/hdblcm --list_systems | egrep 'HDB_ALONE|HDB[0-9][0-9]|version:|hosts?|edition:'")
        else:
            return self.__exec(
                "{0}/hdblcm --list_systems |egrep 'HDB_ALONE|HDB[0-9][0-9]|version:|hosts?|edition:'".format(path))

    def get_all_hana_instance_info(self, server_id, path=None):
        """get instance info for all hana instance"""
        os_output = self.__get_all_hana_instance_info(path)
        if os_output is None:
            Mu.log_warning(self.__logger, "Can not get hana instance info for server:{0}.".format(server_id))
            hana_info = []
        else:
            try:
                i = 0
                hana_info = []
                while i < len(os_output):
                    if "HDB_ALONE" in os_output[i]:
                        info = {Mc.FIELD_SID: os_output[i].split(" ")[0]}
                        i += 1
                        while i < len(os_output) and "HDB_ALONE" not in os_output[i]:
                            if re.match("HDB[0-9][0-9]", os_output[i].strip()):
                                info[Mc.FIELD_INSTANCE_NO] = os_output[i].strip()[3:].strip()
                            elif re.match("hosts?", os_output[i].strip()):
                                info[Mc.FIELD_HOST] = "{0} [{1}]".format(
                                    len(os_output[i].split(":")[1].strip().split(","))
                                    if len(os_output[i].split(":")[1].strip()) > 0 else 0,
                                    os_output[i].split(":")[1].strip())
                            elif "version:" in os_output[i]:
                                info[Mc.FIELD_REVISION] = os_output[i].split(" ")[1].strip()
                            elif "edition:" in os_output[i]:
                                info[Mc.FIELD_EDITION] = os_output[i].split(":")[1].strip()

                            i += 1
                        hana_info.append(info)
                    else:  # fixed the endless loop when the output is not valid
                        i += 1

            except Exception as ex:
                hana_info = []
                Mu.log_warning(self.__logger, "Parsing output failed in 'get_all_hana_instance_info' with error:"
                                              " {0}, server: {1}, the output: {2}".format(ex, server_id, os_output))
        return hana_info

    @staticmethod
    def __exec(command):
        """execute os command and turns the output to list"""
        output = subprocess.getoutput(command)
        return output.split("\n") if output is not None else None


class MsgProducerService:
    """ HANA Server DB operator, responsible for all DB relative operations, it's designed as singleton.
    To get the instance of this class: HANAServerDBOperatorService.instance()
    Initialize the class using HANAServerDBOperatorService() will raise an exception.
    """
    __instance = None

    @staticmethod
    def instance():
        """static access method for singleton"""
        if MsgProducerService.__instance is None:
            MsgProducerService()
        return MsgProducerService.__instance

    def __init__(self):
        # implement the singleton class
        if MsgProducerService.__instance is not None:
            raise MonitorDBOpError("This class is a singleton, use HANAServerDBOperatorService.instance() instead")
        else:
            MsgProducerService.__instance = self
            self.__producer = Ku.get_producer()
            self.__logger = Mu.get_logger(Mc.LOGGER_AGENT_MSG_PRODUCER)
            self.__topic = Mc.TOPIC_MONITORING_INFO
            self.__topic_heartbeat = Mc.TOPIC_AGENT_HEARTBEAT

    def delivery(self, info_list, info_type, server_id):
        try:
            # send heartbeat data
            self.__producer.send(self.__topic_heartbeat, {
                Mc.FIELD_SERVER_ID: server_id, Mc.MSG_TYPE: info_type, Mc.MSG_TIME: Mu.generate_check_id()})
            self.__producer.flush()
            if info_list:
                # as producer doesn't support transaction
                # https://github.com/dpkp/kafka-python/issues/1396
                # https://github.com/dpkp/kafka-python/issues/1063
                # add message begin and end part, consumer will abandon all messages if missing begin or end
                # header
                Mu.log_debug(self.__logger, "Sending {0} message header to queue...".format(info_type))
                self.__producer.send(self.__topic, MsgProducerService.__get_message_header(info_type, server_id))
                # body
                for info in info_list:
                    # for all messages, add type and server id
                    info[Mc.MSG_TYPE] = info_type
                    info[Mc.FIELD_SERVER_ID] = server_id
                    Mu.log_debug(self.__logger, "Sending {0} info {1} to queue...".format(info_type, info))
                    self.__producer.send(self.__topic, info)
                    Mu.log_debug(self.__logger, "{0} info {1} is sent to queue...".format(info_type, info))
                # ending
                Mu.log_debug(self.__logger, "Sending {0} message ending to queue...".format(info_type))
                self.__producer.send(self.__topic, MsgProducerService.__get_message_ending(info_type, server_id))
                self.__producer.flush()
                Mu.log_debug(self.__logger, "Sending {0} message to queue is finished...".format(info_type))
        except Exception as ex:
            Mu.log_error(self.__logger, "Some thing wrong when delivering, error: {0}".format(ex))

    @staticmethod
    def __get_message_header(msg_type, server_id):
        return {Mc.MSG_TYPE: msg_type, Mc.FIELD_SERVER_ID: server_id, Mc.MSG_HEADER: True}

    @staticmethod
    def __get_message_ending(msg_type, server_id):
        return {Mc.MSG_TYPE: msg_type, Mc.FIELD_SERVER_ID: server_id, Mc.MSG_ENDING: True}


class Agent:
    """
    Usage: Agent.py

    --server_id=<server_id>                       server id
    --mount_point=<mount_point>                   mount point of your storage
    --m_frequency=<check_frequency_for_memory>    check frequency for memory in seconds
    --d_frequency=<check_frequency_for_disk>      check frequency for disk in seconds
    --c_frequency=<check_frequency_for_cpu>       check frequency for CPU in seconds
    --i_frequency=<check_frequency_for_instance>  check frequency for instance in seconds

    -h | --help
    """
    short_opts = "h"
    long_opts = ["help", "server_id=", "mount_point=", "q_server_name=", "q_server_port=",
                 "m_frequency=", "d_frequency=", "c_frequency=", "i_frequency="]

    def __init__(self):
        self.PID_FILE = "/tmp/monitor_agent.pid"
        self.server_id = 0
        self.mount_point = '/usr/sap'
        self.m_frequency = 15
        self.d_frequency = 3600
        self.c_frequency = 1800
        self.i_frequency = 600
        self.__logger = Mu.get_logger(Mc.LOGGER_AGENT)

        # register the signals to be caught
        signal.signal(signal.SIGINT, self.terminate_process)  # kill -2
        signal.signal(signal.SIGQUIT, self.terminate_process)  # kill -3
        signal.signal(signal.SIGABRT, self.terminate_process)  # kill -6
        signal.signal(signal.SIGBUS, self.terminate_process)  # kill -7
        signal.signal(signal.SIGFPE, self.terminate_process)  # kill -8
        signal.signal(signal.SIGUSR1, self.terminate_process)  # kill -11
        signal.signal(signal.SIGTERM, self.terminate_process)  # kill -15

        atexit.register(Agent.exit_clean_up, logger=self.__logger, file=self.PID_FILE)

    def start(self):
        self.handle_parameters()
        self.handle_pid_file()
        memory_monitor = MemoryMonitor(self.server_id, self.m_frequency)
        cpu_monitor = CPUMonitor(self.server_id, self.c_frequency)
        disk_monitor = DiskMonitor(self.server_id, self.mount_point, self.d_frequency)
        instance_monitor = InstanceInfoMonitor(self.server_id, self.i_frequency)

        memory_monitor.start()
        cpu_monitor.start()
        disk_monitor.start()
        instance_monitor.start()

    @staticmethod
    def exit_clean_up(logger, file):
        Mu.log_info(logger, "Removing the pid file {0}.".format(file))
        os.unlink(file)

    def terminate_process(self, signal_number, frame=None):
        Mu.log_warning(self.__logger, "Received {0}, exiting...".format(signal_number))
        sys.exit(signal_number)

    def help(self):
        print(self.__doc__)
        sys.exit(0)

    def handle_parameters(self):
        # get all parameters
        try:
            opts, args = getopt.getopt(sys.argv[1:], self.short_opts, self.long_opts)
        except getopt.GetoptError as err:
            print(err)
            print("for help use --help")
            sys.exit(2)

        if opts is None or len(opts) <= 0:
            self.help()
            sys.exit(1)

        try:
            for o, a in opts:
                if o in ("-h", "--help"):
                    self.help()
                elif o in "--server_id":
                    self.server_id = int(a)
                    print("server_id:", self.server_id)
                elif o in "--mount_point":
                    self.mount_point = a
                    print("mount_point:", self.mount_point)
                elif o in "--m_frequency":
                    self.m_frequency = int(a)
                    print("m_frequency:", self.m_frequency)
                elif o in "--d_frequency":
                    self.d_frequency = int(a)
                    print("d_frequency:", self.d_frequency)
                elif o in "--c_frequency":
                    self.c_frequency = int(a)
                    print("c_frequency:", self.c_frequency)
                elif o in "--i_frequency":
                    self.i_frequency = int(a)
                    print("i_frequency:", self.i_frequency)
                else:
                    print("[ERROR]", "Unknown option '%s'" % o)
                    self.help()
                    sys.exit(1)
        except ValueError as err:
            print(err)
            print("for help use --help")
            sys.exit(2)

    def handle_pid_file(self):
        """
        1. check whether there is agent process running, if yes, just kill the previous process
        2. update pid file with current process id at the beginning
        3. delete pid file before exit
        """
        pid_file = self.PID_FILE
        # get pid for current process
        pid = str(os.getpid())
        # get current user name
        user_name = getpass.getuser()

        # pid file exists
        if os.path.isfile(pid_file):
            Mu.log_info(self.__logger, "{0} already exists, trying to kill the previous agent".format(pid_file))
            with open(pid_file) as f:
                old_pid = f.read()
                Mu.log_info(self.__logger, "checking the pid {0} of the previous agent".format(old_pid))
                # get the process list owned by current user, started via python and have the same pid as the pid file
                processes = subprocess.getoutput("ps -fu {0} | grep '[Pp]ython' | grep {1}".format(user_name, old_pid))
                # retry times
                count = 0
                while processes and count < 10 and len(processes.strip()) > 0:
                    Mu.log_info(self.__logger, "trying to kill the pid {0}.".format(old_pid))
                    try:
                        os.kill(int(old_pid), signal.SIGKILL)
                    except ProcessLookupError as ex:
                        Mu.log_warning(self.__logger, "Failed to kill the old agent, error: {0}".format(ex))

                    processes = subprocess.getoutput(
                        "ps -fu {0} | grep '[Pp]ython' | grep {1}".format(user_name, old_pid))
                    count += 1

                if count >= 10:
                    Mu.log_error(self.__logger,
                                 "Some thing wrong happened, can't kill the previous process {0}".format(old_pid))
                    raise MonitorOSOpError(
                        "Some thing wrong happens, can't kill the previous process {0}".format(old_pid))
            # remove the old pid file
            os.unlink(pid_file)

        # write current pid to pid file
        Mu.log_info(self.__logger, "trying to write the pid file {0} with pid {1}.".format(pid_file, pid))
        with open(pid_file, "w") as f:
            f.write(pid)


if __name__ == '__main__':
    # memory_monitor = MemoryMonitor(2, 15)
    # cpu_monitor = CPUMonitor(5, 15)
    # disk_monitor = DiskMonitor(8, '/usr/sap', 3600)
    # instance_monitor = InstanceInfoMonitor(3, 3600)
    #
    # memory_monitor.start()
    # cpu_monitor.start()
    # disk_monitor.start()
    # instance_monitor.start()
    #
    # memory_monitor = MemoryMonitor(1, 15)
    # cpu_monitor = CPUMonitor(4, 15)
    # disk_monitor = DiskMonitor(7, '/usr/sap', 3600)
    # instance_monitor = InstanceInfoMonitor(6, 3600)
    #
    # memory_monitor.start()
    # cpu_monitor.start()
    # disk_monitor.start()
    # instance_monitor.start()

    Agent().start()
