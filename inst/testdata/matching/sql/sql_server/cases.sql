-- cohort
-- 1 M born in 1970
-- 1 F born in 1971

DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
SELECT cohort_definition_id, subject_id, cohort_start_date, cohort_end_date
FROM (
  SELECT
    @target_cohort_id as cohort_definition_id,
    p.person_id as subject_id,
    DATEFROMPARTS(1970, 6, 1) as cohort_start_date,
    op.observation_period_end_date as cohort_end_date,
    ROW_NUMBER() OVER (PARTITION BY p.year_of_birth ORDER BY p.person_id DESC)  row_num
  FROM @cdm_database_schema.person AS p
  LEFT JOIN @cdm_database_schema.observation_period AS op
  ON p.person_id = op.person_id
  WHERE p.year_of_birth = 1970 AND p.gender_concept_id=8507 --M
  OR    p.year_of_birth = 1971 AND p.gender_concept_id=8532 --F
)
WHERE row_num = 1;
