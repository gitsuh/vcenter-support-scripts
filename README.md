# vCenter-support-scripts #

A collection of scripts used to troubleshoot, repair, and enhance VMware vCenter.
----------

### sql_express_purge_events_tasks.sql ###
This script will purge all the vCenter Events and Tasks from the MSSQL database.

Using MSSQL Express database provided with vCenter Windows install supports only up to 50 VMs and 5 ESXi hosts. The database has a hard limit size of 10 GB which can become exhausted depending on task/event retention limits. Once the database limit is exhausted vCenter will constantly shutdown and restart. The SQL script sql_express_purge_events_tasks.sql will purge all the data usually freeing up to 98% of the database capacity.
The following tables usually comprise the majority of the VIM_VCDB database

> dbo.VPX_EVENT
> dbo.VPX_TASK
