
      run 'ToursDB_schema.sql';
      run 'loadTables.sql';
      -- ToursDB_schema.sql turns autocommit off, so commit now.
      commit;
    