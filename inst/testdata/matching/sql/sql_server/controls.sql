-- cohort
-- 10 M born in 1970
-- 10 F born in 1970
-- 10 F born in 1971
-- 10 F born in 1972

DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
SELECT cohort_definition_id, subject_id, cohort_start_date, cohort_end_date
FROM (
  SELECT
    @target_cohort_id as cohort_definition_id,
    p.person_id as subject_id,
    op.observation_period_start_date as cohort_start_date,
    op.observation_period_end_date as cohort_end_date,
    ROW_NUMBER() OVER (PARTITION BY p.year_of_birth, p.gender_concept_id  ORDER BY p.person_id ASC)  row_num
  FROM @cdm_database_schema.person AS p
  LEFT JOIN @cdm_database_schema.observation_period AS op
  ON p.person_id = op.person_id
  WHERE p.year_of_birth = 1970 AND p.gender_concept_id=8507 --M
  OR    p.year_of_birth = 1970 AND p.gender_concept_id=8532 --F
  OR    p.year_of_birth = 1971 AND p.gender_concept_id=8532 --F
  OR    p.year_of_birth = 1972 AND p.gender_concept_id=8532 --F
)
WHERE row_num <= 10;
