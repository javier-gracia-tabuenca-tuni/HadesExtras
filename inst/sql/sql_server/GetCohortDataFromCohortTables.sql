-- Join cohorts from cohort table to person table to get the source_concep_ids
SELECT
  c.cohort_definition_id AS cohort_definition_id ,
  p.person_source_value AS person_source_value,
  c.cohort_start_date AS cohort_start_date,
  c.cohort_end_date AS cohort_end_date
FROM @cohort_database_schema.@cohort_table AS c
LEFT JOIN @cdm_database_schema.person AS p
ON p.person_id = c.subject_id
WHERE cohort_definition_id  IN @cohort_ids
