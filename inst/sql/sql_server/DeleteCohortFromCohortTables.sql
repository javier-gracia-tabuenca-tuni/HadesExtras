
-- Delete cohorts from cohort table with the cohort_definition_id that have the cohort_names_to_delete in cohort table
DELETE FROM @cohort_database_schema.@cohort_table
WHERE cohort_definition_id IN (
    SELECT cohort_definition_id
    FROM @cohort_database_schema.@cohort_name_table
    WHERE cohort_name IN @cohort_names_to_delete
);
