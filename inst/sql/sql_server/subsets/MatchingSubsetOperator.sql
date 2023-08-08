
SELECT
  subject_id,
  cohort_start_date,
  cohort_end_date
{@output_table != ''} ? {INTO @output_table}
FROM(
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY subject_id, cohort_start_date, cohort_end_date)  repeated
  FROM (
    SELECT
     intput_to_match.subject_id,
     {@new_cohort_start_date_as_match == '1'}?{target_matching_rules}:{intput_to_match}.cohort_start_date,
     {@new_cohort_end_date_as_match == '1'}?{target_matching_rules}:{intput_to_match}.cohort_end_date,
     ROW_NUMBER() OVER (PARTITION BY target_matching_rules.subject_id ORDER BY NEWID())  row_num
    FROM(
      SELECT
        S.subject_id,
        p.gender_concept_id,
        p.year_of_birth,
        S.cohort_start_date,
        S.cohort_end_date
      FROM @cohort_database_schema.@cohort_table S
      LEFT JOIN @cdm_database_schema.person p ON S.subject_id = p.person_id
      WHERE S.cohort_definition_id = @match_to_cohort_id
    ) target_matching_rules
    LEFT JOIN (
      SELECT
        T.subject_id,
        p.gender_concept_id,
        p.year_of_birth,
        T.cohort_start_date,
        T.cohort_end_date
      FROM @target_table T
      LEFT JOIN @cdm_database_schema.person p ON T.subject_id = p.person_id
    ) intput_to_match
    ON 1=1 -- simplifies ternary logic
    {@match_sex == '1'} ? {AND target_matching_rules.gender_concept_id = intput_to_match.gender_concept_id}
    {@match_birth_year == '1'} ? {AND target_matching_rules.year_of_birth = intput_to_match.year_of_birth}
    {@match_start_date_with_in_duration == '1'} ? {AND ( intput_to_match.cohort_start_date <= target_matching_rules.cohort_start_date AND target_matching_rules.cohort_start_date <= intput_to_match.cohort_end_date)}
  )
  WHERE row_num <= @match_ratio
)
WHERE repeated = 1

