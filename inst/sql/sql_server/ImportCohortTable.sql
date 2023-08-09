-- This code inserts records from an external cohort table into the cohort table of the cohort database schema.
DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
SELECT @target_cohort_id AS cohort_definition_id, subject_id, cohort_start_date, cohort_end_date
FROM {@is_temp_table} ? {@source_cohort_table} : {@source_database_schema.@source_cohort_table}
WHERE cohort_definition_id = @source_cohort_id
