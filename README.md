# vCenter-support-scripts

A collection of scripts used to troubleshoot, repair, and enhance VMware vCenter.
----------
#### **vcenter_sql_express_purge_events_tasks_stats.sql**
This is modified version of VMware script which does not need any parameters and will delete ALL stats, tasks, and events.
VMware KB: [Selective deletion of tasks, events, and historical performance data in vSphere 5.x and 6.x](https://kb.vmware.com/kb/2110031) 

The following tables are typically the largest in the VIM_VCDB database:

 -  dbo.VPX_EVENT
 -  dbo.VPX_TASK
