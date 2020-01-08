# Script (Python) Part of HANA Server OS Monitoring Tool V2

New features for v2:
* Switch to microservices architecture;
* Support high frequency for memory checking (tested 5 seconds' interval);
* Add emergency shutdown for HANA instance when memory usage exceeds the emergency threshold;
* Perform log backup cleaning for the highest disk consumer if the warning email has been sent for over three times;
* Add heartbeat for all servers/agents.

Functions from [hana_os_monitor_script v1](https://github.com/ckyycc/hana_os_monitor_script):

* Monitor and save all the resources (CPU, Disk and Memory) consumption information (to DB) for all the configured servers;
* Send warning email to the top 5 resource consumers when the threshold of resource consumption is exceeded;
* Send warning email to administrators if some server is not available;
* Try to shutdown HANA (via HDB stop) instance if the warning email has been sent for over three times (only valid for memory monitoring);
* Monitor and save the instance basic info (to DB) for all hana instances, including SID, instance number, host, server name, revision, edition and so on. 


## Design

Check out the architecture diagram for more information:

![architecture_diagram](https://github.com/ckyycc/hana_os_monitor_script_v2/blob/master/design/architecture_diagram.png?raw=true)

Todo
----
* Support manual/scheduled operations (startup/shutdown/logbackup cleaning/...) for HANA instances
* ...

## License
 [MIT](/LICENSE)
