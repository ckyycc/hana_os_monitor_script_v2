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

    def __ssh_exec_command(self, command, ssh, stdin_param=None, backend=False):
        if ssh is None:
            Mu.log_warning(self.__logger, "Skipped Command:{0} for empty ssh connection".format(command))
            return

        try:
            # only enable get_pty without input parameters (the output contains '\r\h' when get_pty=true)
            pty_flag = False if stdin_param is None else True
            if backend:
                command = 'nohup bash -lc "{0}" >/dev/null 2>&1 &'.format(command)

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

    def restart_agent(self, ssh, server_id, mount_point, agent_path,
                      mem_interval, cpu_interval, disk_interval, instance_interval):
        Mu.log_info(self.__logger, "Starting agent at {0} on server {1} with {2} {3} {4} {5} {6}...".format(
            agent_path, server_id, mount_point, mem_interval, cpu_interval, disk_interval, instance_interval))
        self.__ssh_exec_command(
            "python {0} --server_id={1} --mount_point={2} "
            "--m_frequency={3} --d_frequency={4} --c_frequency={5} --i_frequency={6}".format(
                agent_path, server_id, mount_point, mem_interval, cpu_interval, disk_interval, instance_interval),
            ssh, backend=True)
        Mu.log_info(self.__logger, "Finished starting agent at {0} on server {1}.".format(agent_path, server_id))

    def shutdown_hana(self, ssh):
        if Mu.is_test_mod():
            Mu.log_debug(self.__logger, "It's in test mode, skip shutting down hana.")
            return

        cmd_output = self.__ssh_exec_command('nohup bash -lc "HDB stop" >/dev/null 2>&1 &', ssh)
        Mu.log_debug(self.__logger, "shutting down hana, output:{0}".format(cmd_output))

    def clean_log_backup(self, ssh, sid):
        if Mu.is_test_mod():
            Mu.log_debug(self.__logger, "It's in test mode, skip cleaning log backup for {0}.".format(sid))
            return

        self.__ssh_exec_command(
            'find /usr/sap/{0}/HDB[0-9][0-9]/backup -name "log_backup_*.*" -mtime +10 -type f -delete'.format(sid), ssh)
        Mu.log_debug(self.__logger, "cleaned log backup for {0}.".format(sid))


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
