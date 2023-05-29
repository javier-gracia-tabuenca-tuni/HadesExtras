




test_that("getCohortNamesFromCohortTable works", {

  # setup
  testCohortTableName <- "test"
  testCohortNames <- c("A", "B")

  database_settings <- helper_getDatabaseSettings()
  connection <- DatabaseConnector::connect(database_settings$connectionDetails)

  helper_createCohortTableWithTestData(
    connection,
    cohortDatabaseSchema = database_settings$schemas$scratch,
    cohortTableNames = getCohortTableNames(testCohortTableName),
    testCohortNames = testCohortNames
  )

  # call
  cohortNames <- getCohortNamesFromCohortTable(
    connection = connection,
    cohortDatabaseSchema = database_settings$schemas$scratch,
    cohortTableNames = getCohortTableNames(testCohortTableName)
  )

  # tests
  cohortNames |> checkmate::expect_set_equal(testCohortNames)

  # clean up
  DatabaseConnector::dropEmulatedTempTables(connection)
  DatabaseConnector::disconnect(connection)

})

test_that("getCohortTableSummary works", {

  # setup
  testCohortTableName <- "test"
  testCohortNames <- c("A", "B")

  database_settings <- helper_getDatabaseSettings()
  connection <- DatabaseConnector::connect(database_settings$connectionDetails)

  helper_createCohortTableWithTestData(
    connection,
    cohortDatabaseSchema = database_settings$schemas$scratch,
    cohortTableNames = getCohortTableNames(testCohortTableName),
    testCohortNames = testCohortNames
  )

  # call
  cohortTableSummary <- getCohortsSummary(
    connection = connection,
    cohortDatabaseSchema = database_settings$schemas$scratch,
    cdmDatabaseSchema = database_settings$schemas$CDM,
    vocabularyDatabaseSchema = database_settings$schemas$vocab,
    cohortTableNames = getCohortTableNames(testCohortTableName)
  )

  # tests
  cohortTableSummary |> checkmate::expect_tibble()
  cohortTableSummary |> expect_snapshot()

  # clean up
  DatabaseConnector::dropEmulatedTempTables(connection)
  DatabaseConnector::disconnect(connection)

})

#
# appendCohortDataToCohortTable
#

test_that("appendCohortDataToCohortTable works", {

  # setup
  testCohortNames <- c("A", "B")
  testCohortTableName <- "test"

  database_settings <- helper_getDatabaseSettings()
  connection <- DatabaseConnector::connect(database_settings$connectionDetails)

  helper_createCohortTableWithTestData(
    connection,
    cohortDatabaseSchema = database_settings$schemas$scratch,
    cohortTableNames = getCohortTableNames(testCohortTableName),
    testCohortNames = testCohortNames
  )

  # params
  pairedSourcePersonAndPersonIds <- helper_getParedSourcePersonAndPersonIds(
    connection = connection,
    cohortDatabaseSchema = database_settings$schemas$CDM,
    numberPersons = 4
  )

  testCohortData <- tibble::tibble(
    cohort_name = c("C", "C", "D", "D"),
    person_source_value = pairedSourcePersonAndPersonIds$person_source_value,
    cohort_start_date = rep(as.Date(c("2000-01-01", "2010-01-01")), 2),
    cohort_end_date = rep(as.Date(c("2010-01-03", "2020-01-04")), 2)
  )

  # call
  log <- appendCohortDataToCohortTable(
    connection = connection,
    cohortDatabaseSchema = database_settings$schemas$scratch,
    cdmDatabaseSchema = database_settings$schemas$CDM,
    cohortTableNames = getCohortTableNames(testCohortTableName),
    cohortData = testCohortData
  )

  # tests
  log |>
    expect_equal(
      logTibble_NewLog() |>
        logTibble_INFO("Cohort table filled in successfully", "For cohort 'C' 2 patients were added to the cohort table")|>
        logTibble_INFO("Cohort table filled in successfully", "For cohort 'D' 2 patients were added to the cohort table")
    )

  cohortTable <- dplyr::tbl(connection, tmp_inDatabaseSchema(database_settings$schemas$scratch, testCohortTableName)) |> dplyr::collect()
  cohortInfoTable <- dplyr::tbl(connection, tmp_inDatabaseSchema(database_settings$schemas$scratch, paste0(testCohortTableName,"_info")))|> dplyr::collect()

  cohortTable |> expect_snapshot()
  cohortInfoTable |> expect_snapshot()

  # clean up
  DatabaseConnector::dropEmulatedTempTables(connection)
  DatabaseConnector::disconnect(connection)

})
#
#
# test_that("appendCohortDataToCohortTable errors when cohort name exists", {
#
#   test_cohort_table <- "test"
#   database_settings <- helper_getDatabaseSettings(change_cohort_table = test_cohort_table)
#   helper_createCohortTableWithTestData(database_settings, cohort_names = c("A", "B"))
#
#   cohort_data <- tibble::tibble(
#     cohort_name = c("A", "B"),
#     person_source_value = c("001f4a87-70d0-435c-a4b9-1425f6928d33", "052d9254-80e8-428f-b8b6-69518b0ef3f3"),
#     cohort_start_date = as.Date(c("2020-01-01", "2020-01-01")),
#     cohort_end_date = as.Date(c("2020-01-03", "2020-01-04"))
#   )
#
#   appendCohortDataToCohortTable(database_settings, cohort_data) |>
#     expect_error("Cohorts already exists with these names A, B")
# })
#
#
# test_that("appendCohortDataToCohortTable when cohort_start_date fills in with onservation table", {
#
#   test_cohort_table <- "test"
#   database_settings <- helper_getDatabaseSettings(change_cohort_table = test_cohort_table)
#   helper_createCohortTableWithTestData(database_settings, cohort_names = c("A", "B"), n_persons = 2)
#
#   cohort_data <- tibble::tibble(
#     cohort_name = c("C", "C", "D", "D"),
#     person_source_value = helper_getSourcePersonIdFromPersonId(database_settings, c(35,36, 41, 42)),
#     cohort_start_date = as.Date(c(NA, NA, "2000-01-01", "2010-01-01")),
#     cohort_end_date = rep(as.Date(c("2010-01-03", "2020-01-04")), 2)
#   )
#
#   appendCohortDataToCohortTable(database_settings, cohort_data) |>
#     expect_equal(
#       logTibble_NewLog() |>
#         logTibble_WARNING("Filled in cohort_start_date", "For cohort 'C' 2 cohort_start_dates are NA and were replaced by observation_period_start_date")
#     )
#
#   connection <- DatabaseConnector::connect(database_settings$connectionDetails)
#   cohort_table <- dplyr::tbl(connection, tmp_inDatabaseSchema(database_settings$schemas$scratch, database_settings$tables$cohort_table)) |> dplyr::collect()
#   cohort_names_table <- dplyr::tbl(connection, tmp_inDatabaseSchema(database_settings$schemas$scratch, database_settings$tables$cohort_names_table))|> dplyr::collect()
#   DatabaseConnector::disconnect(connection)
#
#   cohort_table |> expect_snapshot()
#   cohort_names_table |> expect_snapshot()
# })
#
#
# test_that("appendCohortDataToCohortTable when cohort_end_date fills in with onservation table", {
#
#   test_cohort_table <- "test"
#   database_settings <- helper_getDatabaseSettings(change_cohort_table = test_cohort_table)
#   helper_createCohortTableWithTestData(database_settings, cohort_names = c("A", "B"), n_persons = 2)
#
#   cohort_data <- tibble::tibble(
#     cohort_name = c("C", "C", "D", "D"),
#     person_source_value = helper_getSourcePersonIdFromPersonId(database_settings, c(35,36, 41, 42)),
#     cohort_start_date = rep(as.Date(c("2000-01-01", "2010-01-01")), 2),
#     cohort_end_date = as.Date(c(NA, NA, "2000-01-01", "2010-01-01"))
#   )
#
#   appendCohortDataToCohortTable(database_settings, cohort_data) |>
#     expect_equal(
#       logTibble_NewLog() |>
#         logTibble_WARNING("Filled in cohort_end_date", "For cohort 'C' 2 cohort_end_dates are NA and were replaced by observation_period_end_date")
#     )
#
#   connection <- DatabaseConnector::connect(database_settings$connectionDetails)
#   cohort_table <- dplyr::tbl(connection, tmp_inDatabaseSchema(database_settings$schemas$scratch, database_settings$tables$cohort_table)) |> dplyr::collect()
#   cohort_names_table <- dplyr::tbl(connection, tmp_inDatabaseSchema(database_settings$schemas$scratch, database_settings$tables$cohort_names_table))|> dplyr::collect()
#   DatabaseConnector::disconnect(connection)
#
#   cohort_table |> expect_snapshot()
#   cohort_names_table |> expect_snapshot()
# })
#
# test_that("appendCohortDataToCohortTable when a cohort is missing all the source ids warns ", {
#
#   test_cohort_table <- "test"
#   database_settings <- helper_getDatabaseSettings(change_cohort_table = test_cohort_table)
#   helper_createCohortTableWithTestData(database_settings, cohort_names = c("A", "B"), n_persons = 2)
#
#   cohort_data <- tibble::tibble(
#     cohort_name = c("C", "C", "D", "D"),
#     person_source_value =  c("notexixst", "notexist", helper_getSourcePersonIdFromPersonId(database_settings, c(41, 42))),
#     cohort_start_date = rep(as.Date(c("2000-01-01", "2010-01-01")), 2),
#     cohort_end_date = rep(as.Date(c("2010-01-03", "2020-01-04")), 2)
#   )
#
#   appendCohortDataToCohortTable(database_settings, cohort_data) |>
#     expect_equal(
#       logTibble_NewLog() |>
#         logTibble_WARNING("source_person_id not found in database", "Cohort 'C' has not been created because none of the source_patient_id were not found in the database")
#     )
#
#   connection <- DatabaseConnector::connect(database_settings$connectionDetails)
#   cohort_table <- dplyr::tbl(connection, tmp_inDatabaseSchema(database_settings$schemas$scratch, database_settings$tables$cohort_table)) |> dplyr::collect()
#   cohort_names_table <- dplyr::tbl(connection, tmp_inDatabaseSchema(database_settings$schemas$scratch, database_settings$tables$cohort_names_table))|> dplyr::collect()
#   DatabaseConnector::disconnect(connection)
#
#   cohort_table |> expect_snapshot()
#   cohort_names_table |> expect_snapshot()
# })
#
#
# test_that("appendCohortDataToCohortTable when a cohort is missing some the source ids warns ", {
#
#   test_cohort_table <- "test"
#   database_settings <- helper_getDatabaseSettings(change_cohort_table = test_cohort_table)
#   helper_createCohortTableWithTestData(database_settings, cohort_names = c("A", "B"), n_persons = 2)
#
#   cohort_data <- tibble::tibble(
#     cohort_name = c("C", "C", "D", "D"),
#     person_source_value =  c("notexixst", helper_getSourcePersonIdFromPersonId(database_settings, c(36, 41, 42))),
#     cohort_start_date = rep(as.Date(c("2000-01-01", "2010-01-01")), 2),
#     cohort_end_date = rep(as.Date(c("2010-01-03", "2020-01-04")), 2)
#   )
#
#   appendCohortDataToCohortTable(database_settings, cohort_data) |>
#     expect_equal(
#       logTibble_NewLog() |>
#         logTibble_WARNING("source_person_id not found in database", "For cohort 'C' 1 source_patient_id were not found in the database")
#     )
#
#   connection <- DatabaseConnector::connect(database_settings$connectionDetails)
#   cohort_table <- dplyr::tbl(connection, tmp_inDatabaseSchema(database_settings$schemas$scratch, database_settings$tables$cohort_table)) |> dplyr::collect()
#   cohort_names_table <- dplyr::tbl(connection, tmp_inDatabaseSchema(database_settings$schemas$scratch, database_settings$tables$cohort_names_table))|> dplyr::collect()
#   DatabaseConnector::disconnect(connection)
#
#   cohort_table |> expect_snapshot()
#   cohort_names_table |> expect_snapshot()
# })
# #
# # test_that("deleteCohortFromCohortTable deletes cohort", {
# #
# #   test_cohort_table <- "test"
# #   database_settings <- helper_getDatabaseSettings(change_cohort_table = test_cohort_table)
# #   helper_createCohortTableWithTestData(database_settings, cohort_names = c("A", "B"))
# #
# #   deleteCohortFromCohortTable(database_settings, "B")
# #
# #   cohort_names <- getCohortTableSummary(database_settings)
# #
# #   cohort_names |> checkmate::expect_tibble()
# #   cohort_names |> expect_equal(
# #     tibble::tibble(
# #       cohort_definition_id = as.integer(c(1, 2)),
# #       cohort_name = c("A", "B"),
# #       atlas_cohort_definition_id = as.integer(c(NA, NA)),
# #       cohort_start_year = as.integer(c(2000, 2000)),
# #       cohort_end_year = as.integer(c(2020, 2020)),
# #       n_entries = as.integer(c(5, 5)),
# #       n_persons = as.integer(c(5, 5))
# #     ) |> dplyr::filter(cohort_name != "B")
# #   )
# #
# # })
# #
# #
#

