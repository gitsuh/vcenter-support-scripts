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

