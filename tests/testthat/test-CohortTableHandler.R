

test_that("CohortTableHandler creates object with correct params", {

  cohortTableHandler <- helper_getNewCohortTableHandler()

  cohortTableHandler |> checkmate::expect_class("CohortTableHandler")
  cohortTableHandler$connectionStatusLog |> checkmate::expect_class("LogTibble")
  cohortTableHandler$connectionStatusLog$logTibble |> dplyr::filter(type != "INFO") |> nrow() |>  expect_equal(0)


})


#
# addCohorts
#
test_that("CohortTableHandler$addCohorts adds a cohort", {

  cohortTableHandler <- helper_getNewCohortTableHandler()

  cohortDefinitionSet <- tibble::tibble(
    cohortId = 10,
    cohortName = "cohort1",
    sql = "DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
    INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    VALUES (@target_cohort_id, 1, CAST('20000101' AS DATE), CAST('20220101' AS DATE)  );",
    json = "ssss"
  )

  cohortTableHandler$addCohorts(cohortDefinitionSet)
  cohortCounts <- cohortTableHandler$getCohortCounts()

  cohortCounts |> expect_equal(
    tibble::tibble(
      cohortName="cohort1", cohortId=10, cohortEntries=1, cohortSubjects=1
    )
  )

})

test_that("CohortTableHandler$addCohorts errors when cohort name exists", {

  cohortTableHandler <- helper_getNewCohortTableHandler()

  cohortDefinitionSet <- tibble::tibble(
    cohortId = 10,
    cohortName = "cohort1",
    sql = "DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
    INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    VALUES (@target_cohort_id, 1, CAST('20000101' AS DATE), CAST('20220101' AS DATE)  );",
    json = "ssss"
  )

  cohortTableHandler$addCohorts(cohortDefinitionSet)

  cohortDefinitionSet$cohortId <- 20
  expect_error( cohortTableHandler$addCohorts(cohortDefinitionSet) )

})

test_that("CohortTableHandler$addCohorts errors when ids name exists", {

  cohortTableHandler <- helper_getNewCohortTableHandler()

  cohortDefinitionSet <- tibble::tibble(
    cohortId = 10,
    cohortName = "cohort1",
    sql = "DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
    INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    VALUES (@target_cohort_id, 1, CAST('20000101' AS DATE), CAST('20220101' AS DATE)  );",
    json = "ssss"
  )

  cohortTableHandler$addCohorts(cohortDefinitionSet)

  cohortDefinitionSet$cohortName <- "Cohort 20"
  expect_error( cohortTableHandler$addCohorts(cohortDefinitionSet) )

})


test_that("CohortTableHandler$addCohorts errors with wrong sql", {

  cohortTableHandler <- helper_getNewCohortTableHandler()

  cohortDefinitionSet <- tibble::tibble(
    cohortId = 10,
    cohortName = "cohort1",
    sql = "DELETE WRONG @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
    INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    VALUES (@target_cohort_id, 1, CAST('20000101' AS DATE), CAST('20220101' AS DATE)  );",
    json = "ssss"
  )

  cohortTableHandler$addCohorts(cohortDefinitionSet)


  expect_error(cohortTableHandler$addCohorts(cohortDefinitionSet))

})






