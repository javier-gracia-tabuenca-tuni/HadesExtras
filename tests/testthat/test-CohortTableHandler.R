

test_that("CohortTableHandler creates object with correct params", {

  cohortTableHandler <- helper_getNewCohortTableHandler()

  cohortTableHandler |> checkmate::expect_class("CohortTableHandler")
  cohortTableHandler$connectionStatusLog |> checkmate::expect_class("LogTibble")
  cohortTableHandler$connectionStatusLog$logTibble |> dplyr::filter(type != "INFO") |> nrow() |>  expect_equal(0)


})


#
# insertOrUpdateCohorts
#
test_that("CohortTableHandler$insertOrUpdateCohorts adds a cohort", {

  cohortTableHandler <- helper_getNewCohortTableHandler()

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

  cohortCounts |> expect_equal(
    tibble::tibble(
      cohortName="cohort1", cohortId=10, cohortEntries=1, cohortSubjects=1
    )
  )

})

test_that("CohortTableHandler$insertOrUpdateCohorts warns when cohort name exists and upadte it", {

  cohortTableHandler <- helper_getNewCohortTableHandler()

  cohortDefinitionSet <- tibble::tibble(
    cohortId = 10,
    cohortName = "cohort1",
    sql = "DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
    INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    VALUES (@target_cohort_id, 1, CAST('20000101' AS DATE), CAST('20220101' AS DATE)  );",
    json = ""
  )

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)

  cohortDefinitionSet$cohortId <- 20
  expect_warning( cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet) )

  cohortsSummary <- cohortTableHandler$getCohortsSummary()
  cohortsSummary |> nrow() |> expect_equal(1)
  cohortsSummary |> dplyr::pull(sourceCohortId) |> expect_equal(20)

})

test_that("CohortTableHandler$insertOrUpdateCohorts changes cohortId and keeps cohortId as sourceCohortId to avoid cohort overlap", {

  cohortTableHandler <- helper_getNewCohortTableHandler()

  cohortDefinitionSet <- tibble::tibble(
    cohortId = 10,
    cohortName = "cohort1",
    sql = "DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
    INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    VALUES (@target_cohort_id, 1, CAST('20000101' AS DATE), CAST('20220101' AS DATE)  );",
    json = ""
  )

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)

  cohortDefinitionSet$cohortName <- "Cohort 20"
  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)

  cohortsSummary <- cohortTableHandler$getCohortsSummary()

  cohortsSummary$cohortId |>  expect_equal(c(10, 20))
  cohortsSummary$sourceCohortId |>  expect_equal(c(10, 10))

})


test_that("CohortTableHandler$insertOrUpdateCohorts errors with wrong sql", {

  cohortTableHandler <- helper_getNewCohortTableHandler()

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

test_that("CohortTableHandler$insertOrUpdateCohorts can insert a cohort with no subjects", {

  cohortTableHandler <- helper_getNewCohortTableHandler()

  cohortDefinitionSet <- tibble::tibble(
    cohortId = 10,
    cohortName = "cohort1",
    sql = "DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;",
    json = ""
  )

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)

  cohortsSummary <- cohortTableHandler$getCohortsSummary()

  cohortsSummary$cohortEntries |> expect_equal(0)
  cohortsSummary$cohortSubjects |> expect_equal(0)
  cohortsSummary$histogram_cohort_start_year |> nrow() |> expect_equal(0)
  cohortsSummary$histogram_cohort_end_year |> nrow() |> expect_equal(0)
  cohortsSummary$count_sex |> nrow() |> expect_equal(0)

})

#
# Delete cohorts
#
test_that("CohortTableHandler$deleteCohorts deletes a cohort and cohortsSummary", {

  cohortTableHandler <- helper_getNewCohortTableHandler()

  cohortDefinitionSet <- tibble::tibble(
    cohortId = c(10,20),
    cohortName = c("cohort1", "cohort2"),
    sql = "DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
    INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    VALUES (@target_cohort_id, 1, CAST('20000101' AS DATE), CAST('20220101' AS DATE)  );",
    json = ""
  )
  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)

  cohortTableHandler$deleteCohorts(10L)

  cohortCounts <- cohortTableHandler$getCohortCounts()

  cohortCounts |> expect_equal(
    tibble::tibble(
      cohortName="cohort2", cohortId=20, cohortEntries=1, cohortSubjects=1
    )
  )

})


#
# cohort summary
#
test_that("CohortTableHandler$cohortsSummary return a tibbe with data", {

  cohortTableHandler <- helper_getNewCohortTableHandler()

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
# test_that("CohortGenerator_generateCohortDefinitionSetToImportFromExternalCohortTable generates cohort definition set", {
#   # Call the function with the inputs
#   cohortDefinitionSet <- CohortGenerator_generateCohortDefinitionSetToImportFromExternalCohortTable(
#     externalCohortDatabaseSchema = "my_external_schema",
#     externalCohortTableName = "my_external_table",
#     externalCohortIds = c(1L, 2L, 3L),
#     externalCohortNames = c("Cohort 1", "Cohort 2", "Cohort 3"),
#     offsetCohortId = 10L,
#     isFromAtlasCohortTable = FALSE
#   )
#
#   # Perform assertions on the result
#
#   cohortDefinitionSet |> checkmate::expect_tibble(
#     types = c("integer64", "character", "character", "character"),
#     any.missing = FALSE,
#     min.rows = 3,
#     max.rows = 3,
#     min.cols = 4,
#     max.cols = 4
#   )
#   cohortDefinitionSet |> purrr::pluck(3,1) |> expect_equal("\n-- This code inserts records from an external cohort table into the cohort table of the cohort database schema.\n\nINSERT INTO @cohort_database_schema.@cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)\nSELECT cohort_definition_id, subject_id, cohort_start_date, cohort_end_date\nFROM my_external_schema.my_external_table\nWHERE cohort_definition_id = 1\n\n")
# })


# test_that("getCohortNamesFromCohortTable works", {

#   # setup
#   testCohortTableName <- "test"
#   testCohortNames <- c("A", "B")

#   database_settings <- helper_getDatabaseSettings()
#   connection <- DatabaseConnector::connect(database_settings$connectionDetails)

#   helper_createCohortTableWithTestData(
#     connection,
#     cohortDatabaseSchema = database_settings$schemas$scratch,
#     cohortTableNames = getCohortTableNames(testCohortTableName),
#     testCohortNames = testCohortNames
#   )

#   # call
#   cohortNames <- getCohortNamesFromCohortTable(
#     connection = connection,
#     cohortDatabaseSchema = database_settings$schemas$scratch,
#     cohortTableNames = getCohortTableNames(testCohortTableName)
#   )

#   # tests
#   cohortNames |> checkmate::expect_set_equal(testCohortNames)

#   # clean up
#   DatabaseConnector::dropEmulatedTempTables(connection)
#   DatabaseConnector::disconnect(connection)

# })

# test_that("getCohortTableSummary works", {

#   # setup
#   testCohortTableName <- "test"
#   testCohortNames <- c("A", "B")

#   database_settings <- helper_getDatabaseSettings()
#   connection <- DatabaseConnector::connect(database_settings$connectionDetails)

#   helper_createCohortTableWithTestData(
#     connection,
#     cohortDatabaseSchema = database_settings$schemas$scratch,
#     cohortTableNames = getCohortTableNames(testCohortTableName),
#     testCohortNames = testCohortNames
#   )

#   # call
#   cohortTableSummary <- getCohortsSummary(
#     connection = connection,
#     cohortDatabaseSchema = database_settings$schemas$scratch,
#     cdmDatabaseSchema = database_settings$schemas$CDM,
#     vocabularyDatabaseSchema = database_settings$schemas$vocab,
#     cohortTableNames = getCohortTableNames(testCohortTableName)
#   )

#   # tests
#   cohortTableSummary |> checkmate::expect_tibble()
#   cohortTableSummary |> expect_snapshot()

#   # clean up
#   DatabaseConnector::dropEmulatedTempTables(connection)
#   DatabaseConnector::disconnect(connection)

# })

# #
# # appendCohortDataToCohortTable
# #

# test_that("appendCohortDataToCohortTable works", {

#   # setup
#   testCohortNames <- c("A", "B")
#   testCohortTableName <- "test"

#   database_settings <- helper_getDatabaseSettings()
#   connection <- DatabaseConnector::connect(database_settings$connectionDetails)

#   helper_createCohortTableWithTestData(
#     connection,
#     cohortDatabaseSchema = database_settings$schemas$scratch,
#     cohortTableNames = getCohortTableNames(testCohortTableName),
#     testCohortNames = testCohortNames
#   )

#   # params
#   pairedSourcePersonAndPersonIds <- helper_getParedSourcePersonAndPersonIds(
#     connection = connection,
#     cohortDatabaseSchema = database_settings$schemas$CDM,
#     numberPersons = 4
#   )

#   testCohortData <- tibble::tibble(
#     cohort_name = c("C", "C", "D", "D"),
#     person_source_value = pairedSourcePersonAndPersonIds$person_source_value,
#     cohort_start_date = rep(as.Date(c("2000-01-01", "2010-01-01")), 2),
#     cohort_end_date = rep(as.Date(c("2010-01-03", "2020-01-04")), 2)
#   )

#   # call
#   log <- appendCohortDataToCohortTable(
#     connection = connection,
#     cohortDatabaseSchema = database_settings$schemas$scratch,
#     cdmDatabaseSchema = database_settings$schemas$CDM,
#     cohortTableNames = getCohortTableNames(testCohortTableName),
#     cohortData = testCohortData
#   )

#   # tests
#   log |>
#     expect_equal(
#       logTibble_NewLog() |>
#         logTibble_INFO("Cohort table filled in successfully", "For cohort 'C' 2 patients were added to the cohort table")|>
#         logTibble_INFO("Cohort table filled in successfully", "For cohort 'D' 2 patients were added to the cohort table")
#     )

#   cohortTable <- dplyr::tbl(connection, tmp_inDatabaseSchema(database_settings$schemas$scratch, testCohortTableName)) |> dplyr::collect()
#   cohortInfoTable <- dplyr::tbl(connection, tmp_inDatabaseSchema(database_settings$schemas$scratch, paste0(testCohortTableName,"_info")))|> dplyr::collect()

#   cohortTable |> expect_snapshot()
#   cohortInfoTable |> expect_snapshot()

#   # clean up
#   DatabaseConnector::dropEmulatedTempTables(connection)
#   DatabaseConnector::disconnect(connection)

# })




# # test_that("appendCohortDataToCohortTable when cohort_start_date fills in with onservation table", {
# #
# #   test_cohort_table <- "test"
# #   database_settings <- helper_getDatabaseSettings(change_cohort_table = test_cohort_table)
# #   helper_createCohortTableWithTestData(database_settings, cohort_names = c("A", "B"), n_persons = 2)
# #
# #   cohort_data <- tibble::tibble(
# #     cohort_name = c("C", "C", "D", "D"),
# #     person_source_value = helper_getSourcePersonIdFromPersonId(database_settings, c(35,36, 41, 42)),
# #     cohort_start_date = as.Date(c(NA, NA, "2000-01-01", "2010-01-01")),
# #     cohort_end_date = rep(as.Date(c("2010-01-03", "2020-01-04")), 2)
# #   )
# #
# #   appendCohortDataToCohortTable(database_settings, cohort_data) |>
# #     expect_equal(
# #       logTibble_NewLog() |>
# #         logTibble_WARNING("Filled in cohort_start_date", "For cohort 'C' 2 cohort_start_dates are NA and were replaced by observation_period_start_date")
# #     )
# #
# #   connection <- DatabaseConnector::connect(database_settings$connectionDetails)
# #   cohort_table <- dplyr::tbl(connection, tmp_inDatabaseSchema(database_settings$schemas$scratch, database_settings$tables$cohort_table)) |> dplyr::collect()
# #   cohort_names_table <- dplyr::tbl(connection, tmp_inDatabaseSchema(database_settings$schemas$scratch, database_settings$tables$cohort_names_table))|> dplyr::collect()
# #   DatabaseConnector::disconnect(connection)
# #
# #   cohort_table |> expect_snapshot()
# #   cohort_names_table |> expect_snapshot()
# # })
# #
# #
# # test_that("appendCohortDataToCohortTable when cohort_end_date fills in with onservation table", {
# #
# #   test_cohort_table <- "test"
# #   database_settings <- helper_getDatabaseSettings(change_cohort_table = test_cohort_table)
# #   helper_createCohortTableWithTestData(database_settings, cohort_names = c("A", "B"), n_persons = 2)
# #
# #   cohort_data <- tibble::tibble(
# #     cohort_name = c("C", "C", "D", "D"),
# #     person_source_value = helper_getSourcePersonIdFromPersonId(database_settings, c(35,36, 41, 42)),
# #     cohort_start_date = rep(as.Date(c("2000-01-01", "2010-01-01")), 2),
# #     cohort_end_date = as.Date(c(NA, NA, "2000-01-01", "2010-01-01"))
# #   )
# #
# #   appendCohortDataToCohortTable(database_settings, cohort_data) |>
# #     expect_equal(
# #       logTibble_NewLog() |>
# #         logTibble_WARNING("Filled in cohort_end_date", "For cohort 'C' 2 cohort_end_dates are NA and were replaced by observation_period_end_date")
# #     )
# #
# #   connection <- DatabaseConnector::connect(database_settings$connectionDetails)
# #   cohort_table <- dplyr::tbl(connection, tmp_inDatabaseSchema(database_settings$schemas$scratch, database_settings$tables$cohort_table)) |> dplyr::collect()
# #   cohort_names_table <- dplyr::tbl(connection, tmp_inDatabaseSchema(database_settings$schemas$scratch, database_settings$tables$cohort_names_table))|> dplyr::collect()
# #   DatabaseConnector::disconnect(connection)
# #
# #   cohort_table |> expect_snapshot()
# #   cohort_names_table |> expect_snapshot()
# # })
# #
# # test_that("appendCohortDataToCohortTable when a cohort is missing all the source ids warns ", {
# #
# #   test_cohort_table <- "test"
# #   database_settings <- helper_getDatabaseSettings(change_cohort_table = test_cohort_table)
# #   helper_createCohortTableWithTestData(database_settings, cohort_names = c("A", "B"), n_persons = 2)
# #
# #   cohort_data <- tibble::tibble(
# #     cohort_name = c("C", "C", "D", "D"),
# #     person_source_value =  c("notexixst", "notexist", helper_getSourcePersonIdFromPersonId(database_settings, c(41, 42))),
# #     cohort_start_date = rep(as.Date(c("2000-01-01", "2010-01-01")), 2),
# #     cohort_end_date = rep(as.Date(c("2010-01-03", "2020-01-04")), 2)
# #   )
# #
# #   appendCohortDataToCohortTable(database_settings, cohort_data) |>
# #     expect_equal(
# #       logTibble_NewLog() |>
# #         logTibble_WARNING("source_person_id not found in database", "Cohort 'C' has not been created because none of the source_patient_id were not found in the database")
# #     )
# #
# #   connection <- DatabaseConnector::connect(database_settings$connectionDetails)
# #   cohort_table <- dplyr::tbl(connection, tmp_inDatabaseSchema(database_settings$schemas$scratch, database_settings$tables$cohort_table)) |> dplyr::collect()
# #   cohort_names_table <- dplyr::tbl(connection, tmp_inDatabaseSchema(database_settings$schemas$scratch, database_settings$tables$cohort_names_table))|> dplyr::collect()
# #   DatabaseConnector::disconnect(connection)
# #
# #   cohort_table |> expect_snapshot()
# #   cohort_names_table |> expect_snapshot()
# # })
# #
# #
# # test_that("appendCohortDataToCohortTable when a cohort is missing some the source ids warns ", {
# #
# #   test_cohort_table <- "test"
# #   database_settings <- helper_getDatabaseSettings(change_cohort_table = test_cohort_table)
# #   helper_createCohortTableWithTestData(database_settings, cohort_names = c("A", "B"), n_persons = 2)
# #
# #   cohort_data <- tibble::tibble(
# #     cohort_name = c("C", "C", "D", "D"),
# #     person_source_value =  c("notexixst", helper_getSourcePersonIdFromPersonId(database_settings, c(36, 41, 42))),
# #     cohort_start_date = rep(as.Date(c("2000-01-01", "2010-01-01")), 2),
# #     cohort_end_date = rep(as.Date(c("2010-01-03", "2020-01-04")), 2)
# #   )
# #
# #   appendCohortDataToCohortTable(database_settings, cohort_data) |>
# #     expect_equal(
# #       logTibble_NewLog() |>
# #         logTibble_WARNING("source_person_id not found in database", "For cohort 'C' 1 source_patient_id were not found in the database")
# #     )
# #
# #   connection <- DatabaseConnector::connect(database_settings$connectionDetails)
# #   cohort_table <- dplyr::tbl(connection, tmp_inDatabaseSchema(database_settings$schemas$scratch, database_settings$tables$cohort_table)) |> dplyr::collect()
# #   cohort_names_table <- dplyr::tbl(connection, tmp_inDatabaseSchema(database_settings$schemas$scratch, database_settings$tables$cohort_names_table))|> dplyr::collect()
# #   DatabaseConnector::disconnect(connection)
# #
# #   cohort_table |> expect_snapshot()
# #   cohort_names_table |> expect_snapshot()
# # })
# # #
# # # test_that("deleteCohortFromCohortTable deletes cohort", {
# # #
# # #   test_cohort_table <- "test"
# # #   database_settings <- helper_getDatabaseSettings(change_cohort_table = test_cohort_table)
# # #   helper_createCohortTableWithTestData(database_settings, cohort_names = c("A", "B"))
# # #
# # #   deleteCohortFromCohortTable(database_settings, "B")
# # #
# # #   cohort_names <- getCohortTableSummary(database_settings)
# # #
# # #   cohort_names |> checkmate::expect_tibble()
# # #   cohort_names |> expect_equal(
# # #     tibble::tibble(
# # #       cohort_definition_id = as.integer(c(1, 2)),
# # #       cohort_name = c("A", "B"),
# # #       atlas_cohort_definition_id = as.integer(c(NA, NA)),
# # #       cohort_start_year = as.integer(c(2000, 2000)),
# # #       cohort_end_year = as.integer(c(2020, 2020)),
# # #       n_entries = as.integer(c(5, 5)),
# # #       n_persons = as.integer(c(5, 5))
# # #     ) |> dplyr::filter(cohort_name != "B")
# # #   )
# # #
# # # })
# # #
# # #
# #


