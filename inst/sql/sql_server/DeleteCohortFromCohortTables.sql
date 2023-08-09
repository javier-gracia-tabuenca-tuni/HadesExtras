-- Delete cohorts from cohort table with the cohort_definition_id that have the cohort_names_to_delete in cohort table
DELETE FROM @cohort_database_schema.@cohort_table where cohort_definition_id IN @cohort_ids;
