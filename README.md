# vCenter-support-scripts

A collection of scripts used to troubleshoot, repair, and enhance VMware vCenter.
----------
#### **sql_express_purge_events_tasks.sql** 
This script will purge all the vCenter Events and Tasks from the MSSQL database.

Using MSSQL Express database provided with vCenter Windows install supports only up to 50 VMs and 5 ESXi hosts. The database has a hard limit size of 10 GB which can become exhausted depending on task/event retention limits. Once the database limit is exhausted vCenter will constantly shutdown and restart. The SQL script sql_express_purge_events_tasks.sql will purge all the data usually freeing up to 98% of the database capacity.
The following tables usually comprise the majority of the VIM_VCDB database:

 -  dbo.VPX_EVENT
 - dbo.VPX_TASK

After purging the database the vCenter retention policies should be adjusted to prevent storaging excessive amounts of events causing capacity problems.



The following lines must be edited if recent tasks, events, and stats should be retained:

    -- ######### INPUT PARAMETERS ########################
    -- Tasks older than @TaskMaxAgeInDays days will be deleted.
    -- If @TaskMaxAgeInDays is 0 all tasks will be deleted
    -- If @TaskMaxAgeInDays has negative value cleanup task step will be skipped
    
    -- Tasks older than @EventMaxAgeInDays days will be deleted.
    -- If @EventMaxAgeInDays is 0 all tasks will be deleted
    -- If @EventMaxAgeInDays has negative value cleanup task step will be skipped
    
    -- Tasks older than @StatMaxAgeInDays days will be deleted.
    -- If @StatMaxAgeInDays is 0 all tasks will be deleted
    -- If @StatMaxAgeInDays has negative value cleanup task step will be skipped

  

    BEGIN TRY
          SET @TaskMaxAgeInDays = CHANGE;
      END TRY
      BEGIN CATCH
           SET @TaskMaxAgeInDays = -1;
      END CATCH
      
      BEGIN TRY
          SET @EventMaxAgeInDays = CHANGE;
      END TRY
      BEGIN CATCH
           SET @EventMaxAgeInDays = -1;
      END CATCH
      
      BEGIN TRY
          SET @StatMaxAgeInDays = CHANGE;
      END TRY
      BEGIN CATCH
           SET @StatMaxAgeInDays = -1;
      END CATCH

