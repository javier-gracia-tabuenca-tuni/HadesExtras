
-- This code inserts records from an external cohort table into the cohort table of the cohort database schema.

INSERT INTO @cohort_database_schema.@cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
SELECT cohort_definition_id, subject_id, cohort_start_date, cohort_end_date
FROM @external_cohort_database_schema.@external_cohort_table_name
WHERE cohort_definition_id = @external_cohort_id

