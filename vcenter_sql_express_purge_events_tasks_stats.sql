-- This is modified version of VMware script which does not need any parameters which will delete ALL stats, tasks, and events.
-- 2110031_MS_SQL_task_event_stat.sql
-- https://kb.vmware.com/kb/2110031

IF EXISTS (SELECT * FROM sysobjects WHERE ID = OBJECT_ID('event_full_cleanup_np') and OBJECTPROPERTY(id, 'IsProcedure') = 1)
   DROP PROCEDURE event_full_cleanup_np;
go
CREATE PROCEDURE event_full_cleanup_np
AS
BEGIN
       BEGIN TRANSACTION
           ALTER TABLE VPX_EVENT_ARG DROP CONSTRAINT FK_VPX_EVENT_ARG_REF_EVENT, FK_VPX_EVENT_ARG_REF_ENTITY
           ALTER TABLE VPX_ENTITY_LAST_EVENT DROP CONSTRAINT FK_VPX_LAST_EVENT_EVENT

           TRUNCATE TABLE VPX_ENTITY_LAST_EVENT
           TRUNCATE TABLE VPX_EVENT_ARG
           TRUNCATE TABLE VPX_EVENT

           ALTER TABLE VPX_EVENT_ARG ADD
                CONSTRAINT FK_VPX_EVENT_ARG_REF_EVENT FOREIGN KEY(EVENT_ID)
                REFERENCES VPX_EVENT (EVENT_ID) ON DELETE CASCADE,
                CONSTRAINT FK_VPX_EVENT_ARG_REF_ENTITY FOREIGN KEY (OBJ_TYPE)
                REFERENCES VPX_OBJECT_TYPE (ID)

            ALTER TABLE VPX_ENTITY_LAST_EVENT ADD
                 CONSTRAINT FK_VPX_LAST_EVENT_EVENT FOREIGN KEY(LAST_EVENT_ID)
                 REFERENCES VPX_EVENT (EVENT_ID) ON DELETE CASCADE
       COMMIT
END
GO
SET QUOTED_IDENTIFIER ON
GO
IF EXISTS (SELECT * FROM sysobjects WHERE ID = OBJECT_ID('event_part_cleanup_np') and OBJECTPROPERTY(id, 'IsProcedure') = 1)
   DROP PROCEDURE event_part_cleanup_np;
go
CREATE PROCEDURE event_part_cleanup_np (@EventCleanupStart as DATETIME)
AS
BEGIN
     exec ('IF OBJECT_ID(''tempdb..#VPX_NEW_EVENT'') IS NOT NULL DROP TABLE #VPX_NEW_EVENT');
     exec ('IF OBJECT_ID(''tempdb..#VPX_NEW_ENTITY_LAST_EVENT'') IS NOT NULL DROP TABLE #VPX_NEW_ENTITY_LAST_EVENT  ');
     exec ('IF OBJECT_ID(''tempdb..#VPX_NEW_EVENT_ARG'') IS NOT NULL DROP TABLE #VPX_NEW_EVENT_ARG');

     BEGIN TRANSACTION
         ALTER TABLE VPX_EVENT_ARG DROP CONSTRAINT FK_VPX_EVENT_ARG_REF_EVENT, FK_VPX_EVENT_ARG_REF_ENTITY
         ALTER TABLE VPX_ENTITY_LAST_EVENT DROP CONSTRAINT FK_VPX_LAST_EVENT_EVENT

        SELECT *
        INTO #VPX_NEW_EVENT
        FROM VPX_EVENT
        WHERE CREATE_TIME >= @EventCleanupStart

        SELECT LE.*
        INTO #VPX_NEW_ENTITY_LAST_EVENT
        FROM VPX_ENTITY_LAST_EVENT LE
        INNER JOIN #VPX_NEW_EVENT E
        ON LE.LAST_EVENT_ID = E.EVENT_ID

        SELECT EA.*
        INTO #VPX_NEW_EVENT_ARG
        FROM VPX_EVENT_ARG EA
        INNER JOIN #VPX_NEW_EVENT E
        ON EA.EVENT_ID = E.EVENT_ID

        TRUNCATE TABLE VPX_ENTITY_LAST_EVENT
        TRUNCATE TABLE VPX_EVENT_ARG
        TRUNCATE TABLE VPX_EVENT

        INSERT INTO VPX_EVENT
        SELECT * FROM #VPX_NEW_EVENT

        INSERT INTO VPX_ENTITY_LAST_EVENT
        SELECT * FROM #VPX_NEW_ENTITY_LAST_EVENT

        INSERT INTO VPX_EVENT_ARG
        SELECT * FROM #VPX_NEW_EVENT_ARG


         ALTER TABLE VPX_EVENT_ARG ADD
            CONSTRAINT FK_VPX_EVENT_ARG_REF_EVENT FOREIGN KEY(EVENT_ID)
            REFERENCES VPX_EVENT (EVENT_ID) ON DELETE CASCADE,
            CONSTRAINT FK_VPX_EVENT_ARG_REF_ENTITY FOREIGN KEY (OBJ_TYPE)
            REFERENCES VPX_OBJECT_TYPE (ID)

         ALTER TABLE VPX_ENTITY_LAST_EVENT ADD
            CONSTRAINT FK_VPX_LAST_EVENT_EVENT FOREIGN KEY(LAST_EVENT_ID)
            REFERENCES VPX_EVENT (EVENT_ID) ON DELETE CASCADE
     COMMIT
END

GO

IF EXISTS (SELECT * FROM sysobjects WHERE ID = OBJECT_ID('event_part_cleanup_p') and OBJECTPROPERTY(id, 'IsProcedure') = 1)
   DROP PROCEDURE event_part_cleanup_p;
go

CREATE PROCEDURE event_part_cleanup_p (@EventCleanupStart as DATETIME)
AS
DECLARE @event_part_num INTEGER
DECLARE @save_event VARCHAR(500)
DECLARE @tr_event VARCHAR(500) 
DECLARE @rs_event VARCHAR(500) 
DECLARE @cl_tmpevent VARCHAR(500) 
DECLARE @save_earg VARCHAR(500)
DECLARE @tr_earg VARCHAR(500) 
DECLARE @rs_earg VARCHAR(500) 
DECLARE @tr_tempearg VARCHAR(500) 



BEGIN 

            SET @event_part_num = 1;
            exec ('IF OBJECT_ID(''tempdb..#VPX_NEW_EVENT'') IS NOT NULL DROP TABLE #VPX_NEW_EVENT  ');
            exec ('IF OBJECT_ID(''tempdb..#VPX_NEW_ENTITY_LAST_EVENT'') IS NOT NULL DROP TABLE #VPX_NEW_ENTITY_LAST_EVENT  ');
            exec ('IF OBJECT_ID(''tempdb..#VPX_NEW_EVENT_ARG'') IS NOT NULL DROP TABLE #VPX_NEW_EVENT_ARG');
            SELECT * INTO #VPX_NEW_EVENT FROM VPX_EVENT_1 WHERE 1=0;
            SELECT * INTO #VPX_NEW_EVENT_ARG FROM VPX_EVENT_ARG_1 WHERE 1=0;
            
             DELETE FROM VPX_EVENT_PARTITION_LOOKUP
             WHERE START_DATE < @EventCleanupStart

              WHILE (@event_part_num <= 92)
                 BEGIN

                    -- Get events with CREATE_DATE > cleanup date
                    SET @save_event = 'INSERT INTO #VPX_NEW_EVENT SELECT * FROM VPX_EVENT_' + CAST(@event_part_num AS VARCHAR(2))
                    SET @save_event = @save_event +' WHERE CAST (CREATE_TIME AS DATE) in (SELECT START_DATE FROM VPX_EVENT_PARTITION_LOOKUP)'
                    
                    -- Truncate all events in the table
                    SET @tr_event = ' TRUNCATE TABLE VPX_EVENT_' +  CAST(@event_part_num AS VARCHAR(2)) 
                    
                    -- Return events with CREATE_DATE > cleanup date into VPX_EVENT_x
                    SET @rs_event = ' INSERT INTO VPX_EVENT_' +  CAST(@event_part_num AS VARCHAR(2)) + ' SELECT * FROM #VPX_NEW_EVENT'
                    
                    -- Truncate temp table
                    SET @cl_tmpevent = ' TRUNCATE TABLE #VPX_NEW_EVENT '
                    
                    -- Get events arguments which exist in VPX_EVENT_x
                    SET @save_earg = 'INSERT INTO #VPX_NEW_EVENT_ARG SELECT * FROM VPX_EVENT_ARG_' +  CAST(@event_part_num AS VARCHAR(2)) 
                    SET @save_earg = @save_earg +' WHERE EVENT_ID IN (SELECT EVENT_ID FROM VPX_EVENT_'+  CAST(@event_part_num AS VARCHAR(2)) + ') ' 
                    
                    -- Truncate all event arguments in the table                    
                    SET @tr_earg = ' TRUNCATE TABLE VPX_EVENT_ARG_' +  CAST(@event_part_num AS VARCHAR(2))  
                    
                    -- Return event arguments which exists in VPX_EVENT_x         
                    SET @rs_earg = ' INSERT INTO VPX_EVENT_ARG_' +  CAST(@event_part_num AS VARCHAR(2)) + ' SELECT * FROM #VPX_NEW_EVENT_ARG'
                    
                    -- Truncate temp table
                    SET @tr_tempearg = ' TRUNCATE TABLE #VPX_NEW_EVENT_ARG '
                    
                    BEGIN TRAN
                      EXEC (@save_event) 
                      EXEC (@tr_event) 
                      EXEC (@rs_event)
                      EXEC (@cl_tmpevent)
                      EXEC (@save_earg)
                      EXEC (@tr_earg)
                      EXEC (@rs_earg)
                      EXEC (@tr_tempearg)
                    COMMIT 
                 SET @event_part_num = @event_part_num + 1;
                 END

              BEGIN TRAN
                   SELECT LE.*
                      INTO #VPX_NEW_ENTITY_LAST_EVENT
                      FROM VPX_ENTITY_LAST_EVENT LE
                      INNER JOIN VPXV_EVENT_ALL E
                        ON LE.LAST_EVENT_ID = E.EVENT_ID
              
                   TRUNCATE TABLE VPX_ENTITY_LAST_EVENT
              
                   INSERT INTO VPX_ENTITY_LAST_EVENT
                  SELECT * FROM #VPX_NEW_ENTITY_LAST_EVENT
              COMMIT


END
GO

IF EXISTS (SELECT * FROM sysobjects WHERE ID = OBJECT_ID('event_full_cleanup_p') and OBJECTPROPERTY(id, 'IsProcedure') = 1)
   DROP PROCEDURE event_full_cleanup_p;
go


CREATE PROCEDURE event_full_cleanup_p
AS

DECLARE @event_part_num VARCHAR(2)
DECLARE @tr_event VARCHAR(500) 
DECLARE @tr_earg VARCHAR(500) 

BEGIN 

   SET @event_part_num = 1
   TRUNCATE TABLE VPX_EVENT_PARTITION_LOOKUP
             
   WHILE (@event_part_num <= 92)
       BEGIN
          -- Truncate all events in the table
          SET @tr_event = ' TRUNCATE TABLE VPX_EVENT_' + @event_part_num 
          -- Truncate all event arguments in the table                    
          SET @tr_earg = ' TRUNCATE TABLE VPX_EVENT_ARG_' + @event_part_num  
          BEGIN TRAN
              EXEC (@tr_event) 
              EXEC (@tr_earg)
          COMMIT
	      SET @event_part_num = @event_part_num +1
        END

        TRUNCATE TABLE VPX_ENTITY_LAST_EVENT

END
go

SET NOCOUNT ON

DECLARE @TaskMaxAgeInDays AS INTEGER
DECLARE @TaskCleanupStart AS DATETIME
DECLARE @EventMaxAgeInDays AS INTEGER
DECLARE @EventCleanupStart AS DATETIME
DECLARE @StatMaxAgeInDays AS INTEGER
DECLARE @StatCleanupStart AS DATETIME
DECLARE @vCRunning AS INTEGER
DECLARE @vCDV AS INTEGER
DECLARE @CleanStats AS INTEGER
DECLARE @table_name VARCHAR(30)
DECLARE @sql_stmt VARCHAR(2000)
DECLARE @affected_rows BIGINT
DECLARE @AllRows AS INTEGER
DECLARE @SaveRows AS INTEGER
DECLARE @ver AS INTEGER

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
    SET @TaskMaxAgeInDays = 0;
END TRY
BEGIN CATCH
     SET @TaskMaxAgeInDays = -1;
END CATCH

BEGIN TRY
    SET @EventMaxAgeInDays = 0;
END TRY
BEGIN CATCH
     SET @EventMaxAgeInDays = -1;
END CATCH

BEGIN TRY
    SET @StatMaxAgeInDays = 0;
END TRY
BEGIN CATCH
     SET @StatMaxAgeInDays = -1;
END CATCH

SET @TaskMaxAgeInDays = ISNULL(@TaskMaxAgeInDays, -1);
SET @EventMaxAgeInDays = ISNULL(@EventMaxAgeInDays, -1);
SET @StatMaxAgeInDays = ISNULL(@StatMaxAgeInDays, -1);

SET @CleanStats = 0;

-- ######### END PARAMETERS ####################

PRINT '---------------------------------------------------------'
PRINT 'Database cleanup may take long time depends from size of '
PRINT 'VPX_TASK, VPX_EVENT, VPX_SAMPLE_TIME1, VPX_SAMPLE_TIME2, '
PRINT 'VPX_SAMPLE_TIME3, VPX_SAMPLE_TIME4                       '
PRINT 'and all VPX_HIST_STATx_y tables                          '
PRINT '---------------------------------------------------------'


------------------------------------------------------------------
-- Tasks clean up
------------------------------------------------------------------

 IF @TaskMaxAgeInDays > 0
    BEGIN
      PRINT '----------------------------------------------------------'
      PRINT 'Starting task cleanup at ' + CAST(getdate() AS VARCHAR(30))
      PRINT '----------------------------------------------------------'

      SET @AllRows = 0;
      SET @SaveRows = 0;
      SET @TaskCleanupStart = getutcdate() - @TaskMaxAgeInDays

      --- The step will be scipped
      --- if @TaskMaxAgeInDays is too big and all the data is after calculated cleanup date
      SELECT @AllRows = count(*),
             @SaveRows = SUM(CASE when QUEUE_TIME >= @TaskCleanupStart THEN 1 ELSE 0 END)
      FROM VPX_TASK

      IF @SaveRows > 0 AND @SaveRows < @AllRows
      BEGIN
	      EXEC ('IF OBJECT_ID(''tempdb..#VPX_NEW_TASK'') IS NOT NULL DROP TABLE #VPX_NEW_TASK ')

	      BEGIN TRANSACTION
		 SELECT *
		 INTO #VPX_NEW_TASK
		 FROM VPX_TASK
		 WHERE QUEUE_TIME >= @TaskCleanupStart

		 INSERT INTO #VPX_NEW_TASK
		 SELECT t.* FROM VPX_TASK t
		 INNER JOIN #VPX_NEW_TASK nt ON nt.parent_task_id=t.task_id
		 WHERE t.TASK_ID NOT IN (SELECT TASK_ID FROM #VPX_NEW_TASK)

		 TRUNCATE TABLE VPX_TASK

		 INSERT INTO VPX_TASK
		 SELECT * FROM #VPX_NEW_TASK
	       COMMIT
      END
      IF @SaveRows = 0
      BEGIN
           BEGIN TRANSACTION
           TRUNCATE TABLE VPX_TASK;
	   COMMIT
      END

   END

 IF @TaskMaxAgeInDays = 0
   BEGIN
       PRINT '----------------------------------------------------------'
       PRINT 'Starting cleanup all tasks at ' + CAST(getdate() AS VARCHAR(30))
       PRINT '----------------------------------------------------------'
       TRUNCATE TABLE VPX_TASK;
   END
------------------------------------------------------------------
-- Events clean up
------------------------------------------------------------------
SELECT @ver = VER_ID FROM VPX_VERSION;
IF @EventMaxAgeInDays > 0
   BEGIN

      PRINT '----------------------------------------------------------'
      PRINT 'Starting event cleanup at ' + CAST(getdate() AS VARCHAR(30))
      PRINT '----------------------------------------------------------'

      SET @AllRows = 0;
      SET @SaveRows = 0;
      SET @EventCleanupStart = getutcdate() - @EventMaxAgeInDays

      --- The step will be scipped
      --- if @EventMaxAgeInDays is too big and all the data is after calculated cleanup date
      

      IF @ver <=600 
          begin
          SELECT @AllRows = count(*),
             @SaveRows = SUM(CASE when CREATE_TIME >= @EventCleanupStart THEN 1 ELSE 0 END)
          FROM VPX_EVENT
          end
      ELSE
          begin
          SELECT @AllRows = count(*),
             @SaveRows = SUM(CASE when CAST(CREATE_TIME AS DATE) >= @EventCleanupStart THEN 1 ELSE 0 END)
          FROM VPXV_EVENT_ALL
          end
       
      IF @SaveRows > 0 AND @SaveRows < @AllRows
          BEGIN

             IF @ver <= 600
                 begin
                 EXEC event_part_cleanup_np @EventCleanupStart
                 end
             ELSE
                 begin
                 EXEC event_part_cleanup_p @EventCleanupStart
                 end

        END
	IF @SaveRows = 0
            BEGIN
              IF @ver <=600  
                   EXEC event_full_cleanup_np 
              ELSE 
                   EXEC event_full_cleanup_p
            END

  END

IF @EventMaxAgeInDays = 0
   BEGIN
       PRINT '----------------------------------------------------------'
       PRINT 'Starting cleanup all events at ' + CAST(getdate() AS VARCHAR(30))
       PRINT '----------------------------------------------------------'

       IF @ver <=600 
             EXEC event_full_cleanup_np
       ELSE
             EXEC event_full_cleanup_p
   END

------------------------------------------------------------------
-- Statistics clean up
------------------------------------------------------------------
 IF  @StatMaxAgeInDays = 0
     BEGIN
        PRINT '----------------------------------------------------------'
        PRINT 'Starting cleanup all statistics  at ' + CAST(getdate() AS VARCHAR(30))
        PRINT '----------------------------------------------------------'

	TRUNCATE TABLE VPX_SAMPLE_TIME1
	TRUNCATE TABLE VPX_SAMPLE_TIME2
	TRUNCATE TABLE VPX_SAMPLE_TIME3
	TRUNCATE TABLE VPX_SAMPLE_TIME4


	DECLARE c_truncate_rest_sql CURSOR FOR
            SELECT name FROM sys.tables
            WHERE name like 'VPX_HIST_STAT%'
        OPEN c_truncate_rest_sql
        FETCH NEXT FROM c_truncate_rest_sql INTO @table_name

	WHILE (@@FETCH_STATUS = 0)
        BEGIN
           SET @sql_stmt = ' TRUNCATE TABLE '+ @table_name
           BEGIN TRAN
           EXEC (@sql_stmt)
           COMMIT
	   FETCH NEXT FROM c_truncate_rest_sql INTO @table_name
        END
        CLOSE c_truncate_rest_sql
        DEALLOCATE c_truncate_rest_sql

     END


IF @StatMaxAgeInDays > 0
   BEGIN
      PRINT '----------------------------------------------------------'
      PRINT 'Starting statistics cleanup at ' + CAST(getdate() AS VARCHAR(30))
      PRINT '----------------------------------------------------------'

      SET @AllRows = 0;
      SET @SaveRows = 0;
      SET @StatCleanupStart = getutcdate() - @StatMaxAgeInDays

      ---- Delete data from VPX_NEW_SAMPLE_TIME1/2/3/4.
      ---- This cause orphan data in VPX_HIST_STAT tables.

      --- The next step will be scipped
      --- if @StatMaxAgeInDays is too big and all the data is after calculated cleanup date
      SELECT @AllRows = count(*),
             @SaveRows = SUM(CASE when SAMPLE_TIME >=  @StatCleanupStart THEN 1 ELSE 0 END)
      FROM VPX_SAMPLE_TIME1

      IF @SaveRows > 0 AND @SaveRows < @AllRows
          BEGIN
             exec ('IF OBJECT_ID(''tempdb..#VPX_NEW_SAMPLE_TIME1'') IS NOT NULL DROP TABLE #VPX_NEW_SAMPLE_TIME1');

	      BEGIN TRANSACTION
		 SELECT *
		 INTO #VPX_NEW_SAMPLE_TIME1
		 FROM VPX_SAMPLE_TIME1
		 WHERE SAMPLE_TIME >= @StatCleanupStart

		 TRUNCATE TABLE VPX_SAMPLE_TIME1

		 INSERT INTO VPX_SAMPLE_TIME1
		 SELECT * FROM #VPX_NEW_SAMPLE_TIME1
	       COMMIT
	       SET @CleanStats = 1;
           END
	IF @SaveRows = 0
	   BEGIN
	      TRUNCATE TABLE VPX_SAMPLE_TIME1
           END

      SET @AllRows = 0;
      SET @SaveRows = 0;
      --- The next step will be scipped
      --- if @StatMaxAgeInDays is too big and all the data is after calculated cleanup date
      SELECT @AllRows = count(*),
             @SaveRows = SUM(CASE when SAMPLE_TIME >=  @StatCleanupStart THEN 1 ELSE 0 END)
      FROM VPX_SAMPLE_TIME2

      IF @SaveRows > 0 AND @SaveRows < @AllRows
          BEGIN
	      exec ('IF OBJECT_ID(''tempdb..#VPX_NEW_SAMPLE_TIME2'') IS NOT NULL DROP TABLE #VPX_NEW_SAMPLE_TIME2');

	      BEGIN TRANSACTION
		 SELECT *
		 INTO #VPX_NEW_SAMPLE_TIME2
		 FROM VPX_SAMPLE_TIME2
		 WHERE SAMPLE_TIME >= @StatCleanupStart

		 TRUNCATE TABLE VPX_SAMPLE_TIME2

		 INSERT INTO VPX_SAMPLE_TIME2
		 SELECT * FROM #VPX_NEW_SAMPLE_TIME2
	       COMMIT
	       SET @CleanStats = 1;
          END
	IF @SaveRows = 0
	   BEGIN
	      TRUNCATE TABLE VPX_SAMPLE_TIME2
           END

      SET @AllRows = 0;
      SET @SaveRows = 0;
      --- The next step will be scipped
      --- if @StatMaxAgeInDays is too big and all the data is after calculated cleanup date
      SELECT @AllRows = count(*),
             @SaveRows = SUM(CASE when SAMPLE_TIME >=  @StatCleanupStart THEN 1 ELSE 0 END)
      FROM VPX_SAMPLE_TIME3

      IF @SaveRows > 0 AND @SaveRows < @AllRows
          BEGIN
	      exec ('IF OBJECT_ID(''tempdb..#VPX_NEW_SAMPLE_TIME3'') IS NOT NULL DROP TABLE #VPX_NEW_SAMPLE_TIME3');

	      BEGIN TRANSACTION
		 SELECT *
		 INTO #VPX_NEW_SAMPLE_TIME3
		 FROM VPX_SAMPLE_TIME3
		 WHERE SAMPLE_TIME >= @StatCleanupStart

		 TRUNCATE TABLE VPX_SAMPLE_TIME3

		 INSERT INTO VPX_SAMPLE_TIME3
		 SELECT * FROM #VPX_NEW_SAMPLE_TIME3
	       COMMIT
	       SET @CleanStats = 1;
          END
	IF @SaveRows = 0
	   BEGIN
	      TRUNCATE TABLE VPX_SAMPLE_TIME3
           END

      SET @AllRows = 0;
      SET @SaveRows = 0;
      --- The next step will be scipped
      --- if @StatMaxAgeInDays is too big and all the data is after calculated cleanup date
      SELECT @AllRows = count(*),
             @SaveRows = SUM(CASE when SAMPLE_TIME >=  @StatCleanupStart THEN 1 ELSE 0 END)
      FROM VPX_SAMPLE_TIME4

      IF @SaveRows > 0 AND @SaveRows < @AllRows
          BEGIN
	      exec ('IF OBJECT_ID(''tempdb..#VPX_NEW_SAMPLE_TIME4'') IS NOT NULL DROP TABLE #VPX_NEW_SAMPLE_TIME4');

	      BEGIN TRANSACTION
		 SELECT *
		 INTO #VPX_NEW_SAMPLE_TIME4
		 FROM VPX_SAMPLE_TIME4
		 WHERE SAMPLE_TIME >= @StatCleanupStart

		 TRUNCATE TABLE VPX_SAMPLE_TIME4

		 INSERT INTO VPX_SAMPLE_TIME4
		 SELECT * FROM #VPX_NEW_SAMPLE_TIME4
	       COMMIT
	       SET @CleanStats = 1;
          END
 	IF @SaveRows = 0
	   BEGIN
	      TRUNCATE TABLE VPX_SAMPLE_TIME4
           END

      ------------------------------------------------------
      -- Delete orphan data from VPX_HIST_STAT tables
      ------------------------------------------------------
      -- Exit if VPX_SAMPLE_TIMEx was not cleaned
      IF @CleanStats = 0 RETURN

      PRINT '----------------------------------------------------------'
      PRINT 'Starting cleanup orphan data at ' + CAST(getdate() AS VARCHAR(30))
      PRINT '----------------------------------------------------------'

      exec ('IF OBJECT_ID(''tempdb..#SAVE_STATS'') IS NOT NULL DROP TABLE #SAVE_STATS');
      CREATE TABLE #SAVE_STATS (TABLE_NAME VARCHAR(30) NOT NULL, TIME_ID BIGINT NOT NULL)

      exec ('IF OBJECT_ID(''tempdb..#DYNAMIC_SQL'') IS NOT NULL DROP TABLE #DYNAMIC_SQL');
      CREATE TABLE #DYNAMIC_SQL (orphan_sql varchar(2000) NOT NULL)

      exec ('IF OBJECT_ID(''tempdb..#VPX_HIST_STAT_TMP'') IS NOT NULL DROP TABLE #VPX_HIST_STAT_TMP');
      CREATE TABLE #VPX_HIST_STAT_TMP (COUNTER_ID bigint, TIME_ID bigint,STAT_VAL bigint )


      -- Populate #DYNAMIC_SQL with scripts for collect orphan rows
      INSERT INTO #DYNAMIC_SQL
      SELECT 'SELECT ' + CHAR(39) + name + CHAR(39) + ', TIME_ID FROM ' + name + ' D WHERE EXISTS (SELECT 1 FROM VPX_SAMPLE_TIME1 M WHERE D.TIME_ID = M.TIME_ID) GROUP BY time_id ' FROM sys.tables WHERE name like 'VPX_HIST_STAT1%'
      INSERT INTO #DYNAMIC_SQL
      SELECT 'SELECT ' + CHAR(39) + name + CHAR(39) + ', TIME_ID FROM ' + name + ' D WHERE EXISTS (SELECT 1 FROM VPX_SAMPLE_TIME2 M WHERE D.TIME_ID = M.TIME_ID) GROUP BY time_id ' FROM sys.tables WHERE name like 'VPX_HIST_STAT2%'
      INSERT INTO #DYNAMIC_SQL
      SELECT 'SELECT ' + CHAR(39) + name + CHAR(39) + ', TIME_ID FROM ' + name + ' D WHERE EXISTS (SELECT 1 FROM VPX_SAMPLE_TIME3 M WHERE D.TIME_ID = M.TIME_ID) GROUP BY time_id ' FROM sys.tables WHERE name like 'VPX_HIST_STAT3%'
      INSERT INTO #DYNAMIC_SQL
      SELECT 'SELECT ' + CHAR(39) + name + CHAR(39) + ', TIME_ID FROM ' + name + ' D WHERE EXISTS (SELECT 1 FROM VPX_SAMPLE_TIME4 M WHERE D.TIME_ID = M.TIME_ID) GROUP BY time_id ' FROM sys.tables WHERE name like 'VPX_HIST_STAT4%'

      DECLARE @l_orphan_sql VARCHAR(2000)
      DECLARE c_exec_sql CURSOR FOR
            SELECT orphan_sql FROM #DYNAMIC_SQL

      OPEN c_exec_sql
      FETCH NEXT FROM c_exec_sql INTO @l_orphan_sql

      SET @affected_rows = 0
      --Populate #SAVE_STATS with table names and time_id. This data should not be deleted.
      WHILE (@@FETCH_STATUS = 0)
        BEGIN
              INSERT INTO #SAVE_STATS
              EXEC (@l_orphan_sql)
              SET @affected_rows = @affected_rows+ @@ROWCOUNT
	          FETCH NEXT FROM c_exec_sql INTO @l_orphan_sql
        END
      CLOSE c_exec_sql
      DEALLOCATE c_exec_sql

      -- Delete orphan data from VPX_HIST_STATx_y which contains orphan and correct data
            IF @affected_rows  > 0
      BEGIN
      DECLARE c_truncate_sql CURSOR FOR
            SELECT table_name FROM #SAVE_STATS group by table_name
                OPEN c_truncate_sql
        FETCH NEXT FROM c_truncate_sql INTO @table_name
        WHILE (@@FETCH_STATUS = 0)
        BEGIN
           BEGIN TRAN
           TRUNCATE TABLE #VPX_HIST_STAT_TMP
           SET @sql_stmt = 'INSERT INTO #VPX_HIST_STAT_TMP'
           SET @sql_stmt = @sql_stmt + ' SELECT * FROM '+ @table_name
           SET @sql_stmt = @sql_stmt + ' WHERE TIME_ID IN (SELECT TIME_ID FROM #SAVE_STATS WHERE TABLE_NAME =''' + @table_name + ''')'
           EXEC (@sql_stmt)
           SET @sql_stmt = ' TRUNCATE TABLE '+ @table_name
           EXEC (@sql_stmt)
           SET @sql_stmt = ' INSERT INTO '+ @table_name + ' SELECT * FROM #VPX_HIST_STAT_TMP'
           EXEC (@sql_stmt)
           COMMIT TRAN

         FETCH NEXT FROM c_truncate_sql INTO @table_name
         END

        CLOSE c_truncate_sql
        DEALLOCATE c_truncate_sql


        -- Delete orphan data from VPX_HIST_STATx_y which contains only orphan data
        DECLARE c_truncate_rest_sql CURSOR FOR
            SELECT name FROM sys.tables
            WHERE name like 'VPX_HIST_STAT%'
             AND name not in (SELECT TABLE_NAME FROM #SAVE_STATS )
        OPEN c_truncate_rest_sql
        FETCH NEXT FROM c_truncate_rest_sql INTO @table_name
        WHILE (@@FETCH_STATUS = 0)
        BEGIN
           SET @sql_stmt = ' TRUNCATE TABLE '+ @table_name
           BEGIN TRAN
           EXEC (@sql_stmt)
           COMMIT
	   FETCH NEXT FROM c_truncate_rest_sql INTO @table_name
        END
        CLOSE c_truncate_rest_sql
        DEALLOCATE c_truncate_rest_sql
     END

PRINT '----------------------------------------------------------'
PRINT 'Done at ' + CAST(getdate() AS VARCHAR(30))
PRINT '----------------------------------------------------------'

END

go
IF EXISTS (SELECT * FROM sysobjects WHERE ID = OBJECT_ID('event_full_cleanup_np') and OBJECTPROPERTY(id, 'IsProcedure') = 1)
   DROP PROCEDURE event_full_cleanup_np;
IF EXISTS (SELECT * FROM sysobjects WHERE ID = OBJECT_ID('event_full_cleanup_np') and OBJECTPROPERTY(id, 'IsProcedure') = 1)
   DROP PROCEDURE event_full_cleanup_np;
IF EXISTS (SELECT * FROM sysobjects WHERE ID = OBJECT_ID('event_part_cleanup_p') and OBJECTPROPERTY(id, 'IsProcedure') = 1)
   DROP PROCEDURE event_part_cleanup_p;
IF EXISTS (SELECT * FROM sysobjects WHERE ID = OBJECT_ID('event_part_cleanup_np') and OBJECTPROPERTY(id, 'IsProcedure') = 1)
   DROP PROCEDURE event_part_cleanup_np;
go
