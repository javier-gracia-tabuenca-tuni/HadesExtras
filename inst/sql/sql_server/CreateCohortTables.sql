
-- Create the cohort table
IF OBJECT_ID('@cohort_database_schema.@cohort_table', 'U') IS NOT NULL
DROP TABLE @cohort_database_schema.@cohort_table;

CREATE TABLE @cohort_database_schema.@cohort_table (
    cohort_definition_id BIGINT,
    subject_id BIGINT,
    cohort_start_date DATE,
    cohort_end_date DATE
);

-- Create the cohort name table
IF OBJECT_ID('@cohort_database_schema.@cohort_name_table', 'U') IS NOT NULL
DROP TABLE @cohort_database_schema.@cohort_name_table;

CREATE TABLE @cohort_database_schema.@cohort_name_table (
    cohort_definition_id BIGINT,
    cohort_name VARCHAR(255),
    atlas_cohort_definition_id BIGINT
);