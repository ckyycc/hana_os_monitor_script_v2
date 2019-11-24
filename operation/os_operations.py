from util import MonitorUtility as Mu
from util import MonitorConst as Mc
import paramiko


class LinuxOperator:
    """Operations for Linux, root class for RedHat and SUSE"""
    def __init__(self):
        # get logging
        self.__logger = Mu.get_logger(Mc.LOGGER_OS_LINUX)

    def open_ssh_connection(self, server_name, user_name, user_password):
        ssh = paramiko.SSHClient()
        try:
            ssh.load_system_host_keys()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            # attach customized attribute for server name,
            # because there is no official way to get the remote server name later.
            ssh.remote_server_name_ = server_name
            ssh.connect(server_name, username=user_name, password=user_password)
        except Exception as ex:
            if ssh is not None:
                ssh.close()
            Mu.log_error(self.__logger,
                         "SSH connection error:{0}! (Server:{1}, User:{2})".format(ex, server_name, user_name))
            # Mu.log_exception(traceback.format_exc())
            ssh = None
        return ssh

    def close_ssh_connection(self, ssh):
        if ssh is not None:
            try:
                ssh.close()
            except Exception as ex:
                server_name = ""
                if ssh is not None and hasattr(ssh, "remote_server_name_"):
                    server_name = " on server:{0}".format(ssh.remote_server_name_)
                Mu.log_error(self.__logger, "Failed to close SSH connection with error:{0}{1}.".format(ex, server_name))

    def __ssh_exec_command(self, command, ssh, stdin_param=None):
        try:
            # only enable get_pty without input parameters (the output contains '\r\h' when get_pty=true)
            pty_flag = False if stdin_param is None else True

            # execute command via SSH with 10 minutes timeout
            cmd_input, cmd_output, cmd_err = ssh.exec_command(command, timeout=600, get_pty=pty_flag)
            if stdin_param is not None:
                cmd_input.write(stdin_param)
                cmd_input.write("\n")
                cmd_input.flush()
            return cmd_output.readlines()
        except Exception as ex:
            server_name = ""
            if ssh is not None and hasattr(ssh, "remote_server_name_"):
                server_name = " on {0}".format(ssh.remote_server_name_)

            Mu.log_warning(self.__logger, "Command:{0} execution failed{1}, {2}".format(command, server_name, ex))

    def restart_agent(self, ssh, server_id, mount_point, mem_interval, cpu_interval, disk_interval, instance_interval):
        # TODO agent path
        self.__ssh_exec_command(
            "python /usr/sap/CK/home/tmp/py_project_25/agent.py --server_id={0} --mount_point={1} "
            "--m_frequency={2} --d_frequency={3} --c_frequency={4} --d_frequency={5}".format(
                server_id, mount_point, mem_interval, cpu_interval, disk_interval, instance_interval), ssh)

    def collect_disk_info(self, ssh, mount_point):
        return self.__ssh_exec_command(
            "df -l| grep .*'\s'{0}$ | {1}".format(mount_point, "awk '{print $(NF-4) \" \" $(NF-2)}'"), ssh)

    def collect_mem_info(self, ssh):
        """ get the overall memory information for system"""
        # cmd_output = self.__ssh_exec_command("cat /proc/meminfo", ssh)
        return self.__ssh_exec_command("free | tail -3 | xargs | awk '{print $2 \" \"  $11}'", ssh)

    def collect_cpu_info(self, ssh):
        """ get the overall CPU information for system"""
        # get CPU Number
        cmd_output_cpu_number = self.__ssh_exec_command("getconf _NPROCESSORS_ONLN", ssh)

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
        cmd_output_cpu_usage = self.__ssh_exec_command(
            "mpstat 1 1 | awk '{for(i=1;i<=NF;i++) {if ($i ~ /%idle/) field=i}} /all/ {print 100 - $field; exit}'", ssh)

        return cmd_output_cpu_number, cmd_output_cpu_usage

    def collect_os_info(self, ssh):
        # get OS version
        cmd_output_os_version = self.__ssh_exec_command("cat /etc/*-release | grep PRETTY_NAME", ssh)
        # get kernel version
        cmd_output_kernel_version = self.__ssh_exec_command("uname -r", ssh)

        return cmd_output_os_version, cmd_output_kernel_version

    def get_mem_consumers(self, ssh):
        return self.__ssh_exec_command("cut -d: -f1 /etc/passwd | grep ^[A-Za-z0-9][A-Za-z0-9][A-Za-z0-9][a]dm$ "
                                       "| awk '{for (i=1;i<=NF;i++) {system(\"ps -Ao user,comm,pid,pmem | "
                                       "grep \"$i\"  | sort -k 4 -nr | head -5\")}}'", ssh)

    def get_cpu_consumers(self, ssh):
        return self.__ssh_exec_command("cut -d: -f1 /etc/passwd | grep ^[A-Za-z0-9][A-Za-z0-9][A-Za-z0-9][a]dm$ "
                                       "| awk '{for (i=1;i<=NF;i++) {system(\"top -u \"$i\" -bn1 | "
                                       "sed -n '8,12'p\")}}'", ssh)

    def get_disk_consumers(self, ssh, mount_point, is_sudosudoers=False, neeed_sudo_pwd=False, os_pwd=None):
        """ Get the disk consuming information for /usr/sap"""

        if is_sudosudoers:
            # sometimes if sudo doesn't need password, in some servers (eg:ls9303), the password
            # will be written to output. So we need to split servers with different sudo password behavior
            # exclude mount_point/tmp: --exclude=/usr/sap/tmp. It's for the hanging issue of llbpal97,
            # modified at 2018/07/22
            # exclude mount_point/shared: --exclude=/usr/sap/shared. It's for the hanging issue of vanpghana06 and 07
            # modified at 2018/07/26
            # exclude mount_point/temp. It's for hanging issue of llbpal96
            # modified at 2019/07/05
            sudo_password = os_pwd if neeed_sudo_pwd else None
            cmd_output = self.__ssh_exec_command(
                "sudo du --exclude={0}/tmp --exclude={0}/temp --exclude={0}/shared --exclude=/usr/sap/eua_paths "
                "--max-depth=1 {0}".format(mount_point),
                ssh,
                sudo_password)
        else:
            # exclude mount_point/tmp: --exclude=/usr/sap/tmp. It's for the hanging issue of llbpal97,
            # modified at 2018/07/22
            # exclude mount_point/shared: --exclude=/usr/sap/shared. It's for the hanging issue of vanpghana06 and 07
            # modified at 2018/07/26
            # exclude mount_point/temp. It's for hanging issue of llbpal96
            # modified at 2019/07/05
            cmd_output = self.__ssh_exec_command(
                "du --exclude={0}/tmp --exclude={0}/temp --exclude={0}/shared --exclude=/usr/sap/eua_paths "
                "--max-depth=1 {0} 2>>/dev/null".format(mount_point), ssh)

        return cmd_output

    def get_all_hana_instance_info(self, ssh, path):
        # switch to hdblcm because it contains nodes num info and will not hang when scanning some share folder
        # but needs to copy hdblcm to all home/bin path or use some share folder
        if path is None:
            return self.__ssh_exec_command(
                "~/bin/hdblcm --list_systems | egrep 'HDB_ALONE|HDB[0-9][0-9]|version:|hosts?|edition:'", ssh)
        else:
            return self.__ssh_exec_command(
                "{0}/hdblcm --list_systems |egrep 'HDB_ALONE|HDB[0-9][0-9]|version:|hosts?|edition:'".format(path), ssh)

    def get_owners_of_sub_folders(self, ssh, folder):
        # get owner of the all folders in <folder>
        return self.__ssh_exec_command("".join(["ls -ld {0}/* | awk ".format(folder), "'{print $3\"\t\"$NF}'"]), ssh)

    def get_all_sid_users(self, ssh):
        return self.__ssh_exec_command("cut -d: -f1 /etc/passwd | grep ^[A-Za-z0-9][A-Za-z0-9][A-Za-z0-9][a]dm$", ssh)

    def shutdown_hana(self, ssh):
        # cmd_output = self.__ssh_exec_command('nohup bash -lc "HDB stop" >/dev/null 2>&1 &', ssh)
        # Mu.log_debug(self.__logger, "shutting down hana, output:{0}".format(cmd_output))
        pass


class SUSEOperator(LinuxOperator):
    """Operations for SUSE Linux
    Will overwrite some functions which need SUSE specific command/parsing"""
    def __init__(self):
        super().__init__()


class RedHatOperator(LinuxOperator):
    """Operations for RedHat Linux
    Will overwrite some functions which need RedHat specific command/parsing"""
    def __init__(self):
        super().__init__()
