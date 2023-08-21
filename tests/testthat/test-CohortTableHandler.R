

test_that("CohortTableHandler creates object with correct params", {

  cohortTableHandler <- helper_getNewCohortTableHandler()
  on.exit({rm(cohortTableHandler);gc()})

  cohortTableHandler |> checkmate::expect_class("CohortTableHandler")
  cohortTableHandler$connectionStatusLog |> checkmate::expect_class("LogTibble")
  cohortTableHandler$connectionStatusLog$logTibble |> dplyr::filter(type != "INFO") |> nrow() |>  expect_equal(0)

})


#
# insertOrUpdateCohorts
#
test_that("CohortTableHandler$insertOrUpdateCohorts adds a cohort", {

  cohortTableHandler <- helper_getNewCohortTableHandler()
  on.exit({rm(cohortTableHandler);gc()})

  cohortDefinitionSet <- tibble::tibble(
    cohortId = 10,
    cohortName = "cohort1",
    sql = "DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
    INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    VALUES (@target_cohort_id, 1, CAST('20000101' AS DATE), CAST('20220101' AS DATE)  );",
    json = ""
  )

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)
  cohortCounts <- cohortTableHandler$getCohortCounts()

  cohortCounts |> checkmate::expect_tibble(nrows = 1)
  cohortCounts$cohortName |> expect_equal("cohort1")
  cohortCounts$cohortId |> expect_equal(10)
  cohortCounts$cohortEntries |> expect_equal(1)
  cohortCounts$cohortSubjects |> expect_equal(1)

})

test_that("CohortTableHandler$insertOrUpdateCohorts warns when cohortId exists and upadte it", {

  cohortTableHandler <- helper_getNewCohortTableHandler()
  on.exit({rm(cohortTableHandler);gc()})

  cohortDefinitionSet <- tibble::tibble(
    cohortId = 10,
    cohortName = "cohort1",
    sql = "DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
    INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    VALUES (@target_cohort_id, 1, CAST('20000101' AS DATE), CAST('20220101' AS DATE)  );",
    json = ""
  )

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)
  cohortCounts <- cohortTableHandler$getCohortCounts()

  cohortCounts |> checkmate::expect_tibble(nrows = 1)
  cohortCounts$cohortName |> expect_equal("cohort1")
  cohortCounts$cohortId |> expect_equal(10)
  cohortCounts$cohortEntries |> expect_equal(1)
  cohortCounts$cohortSubjects |> expect_equal(1)

  cohortDefinitionSet$cohortName <- "cohort1 Updated"
  expect_warning( cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet) )
  cohortCounts <- cohortTableHandler$getCohortCounts()

  cohortCounts |> checkmate::expect_tibble(nrows = 1)
  cohortCounts$cohortName |> expect_equal("cohort1 Updated")
  cohortCounts$cohortId |> expect_equal(10)
  cohortCounts$cohortEntries |> expect_equal(1)
  cohortCounts$cohortSubjects |> expect_equal(1)

})


test_that("CohortTableHandler$insertOrUpdateCohorts errors with wrong sql", {

  cohortTableHandler <- helper_getNewCohortTableHandler()
  on.exit({rm(cohortTableHandler);gc()})

  cohortDefinitionSet <- tibble::tibble(
    cohortId = 10,
    cohortName = "cohort1",
    sql = "DELETE WRONG @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
    INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    VALUES (@target_cohort_id, 1, CAST('20000101' AS DATE), CAST('20220101' AS DATE)  );",
    json = ""
  )

  expect_error(cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet))

})

test_that("CohortTableHandler$insertOrUpdateCohorts can insert a cohort with no subjects, and summary returns a sanitised tibble", {

  cohortTableHandler <- helper_getNewCohortTableHandler()
  on.exit({rm(cohortTableHandler);gc()})

  cohortDefinitionSet <- tibble::tibble(
    cohortId = 10,
    cohortName = "cohort1",
    sql = "DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;",
    json = ""
  )

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)

  cohortsSummary <- cohortTableHandler$getCohortsSummary()

  cohortsSummary |> checkCohortsSummary() |> expect_true()
  cohortsSummary$cohortEntries |> expect_equal(0)
  cohortsSummary$cohortSubjects |> expect_equal(0)
  cohortsSummary$histogramCohortStartYear[[1]] |> nrow() |> expect_equal(0)
  cohortsSummary$histogramCohortEndYear[[1]] |> nrow() |> expect_equal(0)
  cohortsSummary$sexCounts[[1]] |> nrow() |> expect_equal(0)
  cohortsSummary$buildInfo[[1]]$logTibble$message |> expect_equal("Cohort is empty")

})

#
# Delete cohorts
#
test_that("CohortTableHandler$deleteCohorts deletes a cohort and cohortsSummary", {

  cohortTableHandler <- helper_getNewCohortTableHandler()
  on.exit({rm(cohortTableHandler);gc()})

  cohortDefinitionSet <- tibble::tibble(
    cohortId = c(10,20),
    cohortName = c("cohort1", "cohort2"),
    sql = "DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
    INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    VALUES (@target_cohort_id, 1, CAST('20000101' AS DATE), CAST('20220101' AS DATE)  );",
    json = ""
  )
  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)

  cohortTableHandler$getCohortsSummary() |> checkCohortsSummary() |> expect_true()
  cohortTableHandler$getCohortsSummary()$cohortId |> expect_equal(c(10,20))
  cohortTableHandler$cohortDefinitionSet$cohortId |> expect_equal(c(10,20))
  cohortTableHandler$cohortGeneratorResults$cohortId |> expect_equal(c(10,20))
  cohortTableHandler$cohortDemograpics$cohortId |> expect_equal(c(10,20))

  cohortTableHandler$deleteCohorts(10L)

  cohortTableHandler$getCohortsSummary() |> checkCohortsSummary() |> expect_true()
  cohortTableHandler$getCohortsSummary()$cohortId |> expect_equal(c(20))
  cohortTableHandler$cohortDefinitionSet$cohortId |> expect_equal(c(20))
  cohortTableHandler$cohortGeneratorResults$cohortId |> expect_equal(c(20))
  cohortTableHandler$cohortDemograpics$cohortId |> expect_equal(c(20))

})


#
# cohort summary
#
test_that("CohortTableHandler$cohortsSummary return a tibbe with data", {

  cohortTableHandler <- helper_getNewCohortTableHandler()
  on.exit({rm(cohortTableHandler);gc()})

  cohortDefinitionSet <- tibble::tibble(
    cohortId = c(10,20),
    cohortName = c("cohort1", "cohort2"),
    sql = "DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
    INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    VALUES (@target_cohort_id, 1, CAST('20000101' AS DATE), CAST('20220101' AS DATE)  );",
    json = ""
  )
  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)

  cohortsSummary <- cohortTableHandler$getCohortsSummary()

  cohortsSummary |> checkCohortsSummary() |> expect_true()


})

#
