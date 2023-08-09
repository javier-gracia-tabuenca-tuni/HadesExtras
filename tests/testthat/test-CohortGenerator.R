
#
# CohortGenerator_deleteCohortFromCohortTable
#
test_that("CohortGenerator_deleteCohortFromCohortTable deletes a cohort", {

  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  cdmDatabaseSchema <- "main"
  cohortDatabaseSchema <- "main"
  cohortTable <- "cohort"
  Eunomia::createCohorts(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable
  )

  CohortGenerator_deleteCohortFromCohortTable(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = CohortGenerator::getCohortTableNames(cohortTable),
    cohortIds = c(1L,2L)
  )

  codeCounts <- CohortGenerator::getCohortCounts(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable
  )

  codeCounts$cohortId |> expect_equal(c(3L,4L))
})


#
# CohortGenerator_generateCohortSet
#
test_that("cohortDataToCohortDefinitionSet works", {
  # get test settings
  connection <- helper_getConnection()
  CohortGenerator::createCohortTables(
    connection = connection,
    cohortDatabaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema
  )
  on.exit({DatabaseConnector::dropEmulatedTempTables(connection); DatabaseConnector::disconnect(connection)})

  # test params
  sourcePersonToPersonId <- helper_getParedSourcePersonAndPersonIds(
    connection = connection,
    cohortDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    numberPersons = 2*5
  )

  cohort_data <- tibble::tibble(
    cohort_name = rep(c("Cohort A", "Cohort B"), 5),
    person_source_value = sourcePersonToPersonId$person_source_value,
    cohort_start_date = rep(as.Date(c("2020-01-01", "2020-01-01")), 5),
    cohort_end_date = rep(as.Date(c("2020-01-03", "2020-01-04")), 5)
  )

  cohortDefinitionSet <- cohortDataToCohortDefinitionSet(
    cohortData = cohort_data
  )

  # function
  generateCohortSetResults <- CohortGenerator_generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    cohortDefinitionSet = cohortDefinitionSet
  )


  # expectations
  generateCohortSetResults |> checkmate::expect_tibble()
  generateCohortSetResults |> names() |> checkmate::expect_subset(c("cohortName","cohortId","generationStatus", "startTime","endTime","extraInfo"))

  generateCohortSetResults |> tidyr::unnest(extraInfo) |>
    dplyr::select(n_source_person, n_source_entries, n_missing_source_person, n_missing_cohort_start, n_missing_cohort_end) |>
    expect_equal(tibble::tibble(
      n_source_person = c(5, 5),
      n_source_entries = c(5, 5),
      n_missing_source_person = c(0, 0),
      n_missing_cohort_start = c(0, 0),
      n_missing_cohort_end = c(0, 0)
    ))

})

test_that("cohortDataToCohortDefinitionSet reports missing source person id", {
  # get test settings
  connection <- helper_getConnection()
  CohortGenerator::createCohortTables(
    connection = connection,
    cohortDatabaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema
  )
  on.exit({DatabaseConnector::dropEmulatedTempTables(connection); DatabaseConnector::disconnect(connection)})

  # test params
  sourcePersonToPersonId <- helper_getParedSourcePersonAndPersonIds(
    connection = connection,
    cohortDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    numberPersons = 2*5
  )

  cohort_data <- tibble::tibble(
    cohort_name = rep(c("Cohort A", "Cohort B"), 5),
    person_source_value = c(sourcePersonToPersonId$person_source_value[1:5], letters[1:5]),
    cohort_start_date = rep(as.Date(c("2020-01-01", "2020-01-01")), 5),
    cohort_end_date = rep(as.Date(c("2020-01-03", "2020-01-04")), 5)
  )

  cohortDefinitionSet <- cohortDataToCohortDefinitionSet(
    cohortData = cohort_data
  )

  # function
  generateCohortSetResults <- CohortGenerator_generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    cohortDefinitionSet = cohortDefinitionSet
  )

  # expectations
  generateCohortSetResults |> tidyr::unnest(extraInfo) |>
    dplyr::select(n_source_person, n_source_entries, n_missing_source_person, n_missing_cohort_start, n_missing_cohort_end) |>
    expect_equal(tibble::tibble(
      n_source_person = c(5, 5),
      n_source_entries = c(5, 5),
      n_missing_source_person = c(2, 3),
      n_missing_cohort_start = c(0, 0),
      n_missing_cohort_end = c(0, 0)
    ))

})

test_that("cohortDataToCohortDefinitionSet reports missing cohort_start_date", {
  # get test settings
  connection <- helper_getConnection()
  CohortGenerator::createCohortTables(
    connection = connection,
    cohortDatabaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema
  )
  on.exit({DatabaseConnector::dropEmulatedTempTables(connection); DatabaseConnector::disconnect(connection)})

  # test params
  sourcePersonToPersonId <- helper_getParedSourcePersonAndPersonIds(
    connection = connection,
    cohortDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    numberPersons = 2*5
  )

  cohort_data <- tibble::tibble(
    cohort_name = rep(c("Cohort A", "Cohort B"), 5),
    person_source_value = sourcePersonToPersonId$person_source_value,
    cohort_start_date = rep(as.Date(c("2020-01-01", NA)), 5),
    cohort_end_date = rep(as.Date(c("2020-01-03", "2020-01-04")), 5)
  )

  cohortDefinitionSet <- cohortDataToCohortDefinitionSet(
    cohortData = cohort_data
  )

  # function
  generateCohortSetResults <- CohortGenerator_generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    cohortDefinitionSet = cohortDefinitionSet
  )

  # expectations
  generateCohortSetResults |> tidyr::unnest(extraInfo) |>
    dplyr::select(n_source_person, n_source_entries, n_missing_source_person, n_missing_cohort_start, n_missing_cohort_end) |>
    expect_equal(tibble::tibble(
      n_source_person = c(5, 5),
      n_source_entries = c(5, 5),
      n_missing_source_person = c(0, 0),
      n_missing_cohort_start = c(0, 5),
      n_missing_cohort_end = c(0, 0)
    ))

})

test_that("cohortDataToCohortDefinitionSet reports missing cohort_end_date", {
  # get test settings
  connection <- helper_getConnection()
  CohortGenerator::createCohortTables(
    connection = connection,
    cohortDatabaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema
  )
  on.exit({DatabaseConnector::dropEmulatedTempTables(connection); DatabaseConnector::disconnect(connection)})

  # test params
  sourcePersonToPersonId <- helper_getParedSourcePersonAndPersonIds(
    connection = connection,
    cohortDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    numberPersons = 2*5
  )

  cohort_data <- tibble::tibble(
    cohort_name = rep(c("Cohort A", "Cohort B"), 5),
    person_source_value = sourcePersonToPersonId$person_source_value,
    cohort_start_date = rep(as.Date(c("2020-01-01", "2020-01-04")), 5),
    cohort_end_date = rep(as.Date(c(NA, "2020-01-04")), 5)
  )

  cohortDefinitionSet <- cohortDataToCohortDefinitionSet(
    cohortData = cohort_data
  )

  # function
  generateCohortSetResults <- CohortGenerator_generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    cohortDefinitionSet = cohortDefinitionSet
  )

  # expectations
  generateCohortSetResults |> tidyr::unnest(extraInfo) |>
    dplyr::select(n_source_person, n_source_entries, n_missing_source_person, n_missing_cohort_start, n_missing_cohort_end) |>
    expect_equal(tibble::tibble(
      n_source_person = c(5, 5),
      n_source_entries = c(5, 5),
      n_missing_source_person = c(0, 0),
      n_missing_cohort_start = c(0, 0),
      n_missing_cohort_end = c(5, 0)
    ))

})


test_that("cohortDataToCohortDefinitionSet also works from imported files", {
  # get test settings
  connection <- helper_getConnection()
  CohortGenerator::createCohortTables(
    connection = connection,
    cohortDatabaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema
  )
  on.exit({DatabaseConnector::dropEmulatedTempTables(connection); DatabaseConnector::disconnect(connection)})

  # test params
  sourcePersonToPersonId <- helper_getParedSourcePersonAndPersonIds(
    connection = connection,
    cohortDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    numberPersons = 2*5
  )

  cohort_data <- tibble::tibble(
    cohort_name = rep(c("Cohort A", "Cohort B"), 5),
    person_source_value = sourcePersonToPersonId$person_source_value,
    cohort_start_date = rep(as.Date(c("2020-01-01", "2020-01-04")), 5),
    cohort_end_date = rep(as.Date(c(NA, "2020-01-04")), 5)
  )

  cohortDefinitionSet <- cohortDataToCohortDefinitionSet(
    cohortData = cohort_data
  )

  # save and load
  CohortGenerator::saveCohortDefinitionSet(
    cohortDefinitionSet,
    settingsFileName = file.path(tempdir(), "inst/Cohorts.csv"),
    jsonFolder = file.path(tempdir(),"inst/cohorts"),
    sqlFolder = file.path(tempdir(),"inst/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortId"),
    subsetJsonFolder = file.path(tempdir(),"inst/cohort_subset_definitions/")
  )

  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
    settingsFileName = file.path(tempdir(), "inst/Cohorts.csv"),
    jsonFolder = file.path(tempdir(),"inst/cohorts"),
    sqlFolder = file.path(tempdir(),"inst/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortId"),
    subsetJsonFolder = file.path(tempdir(),"inst/cohort_subset_definitions/")
  )

  # function
  generateCohortSetResults <- CohortGenerator_generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    cohortDefinitionSet = cohortDefinitionSet
  )

  # expectations
  generateCohortSetResults |> tidyr::unnest(extraInfo) |>
    dplyr::select(n_source_person, n_source_entries, n_missing_source_person, n_missing_cohort_start, n_missing_cohort_end) |>
    expect_equal(tibble::tibble(
      n_source_person = c(5, 5),
      n_source_entries = c(5, 5),
      n_missing_source_person = c(0, 0),
      n_missing_cohort_start = c(0, 0),
      n_missing_cohort_end = c(5, 0)
    ))

})


test_that("cohortDataToCohortDefinitionSet incremental mode do not create the tmp cohortData", {
  # get test settings
  connection <- helper_getConnection()
  CohortGenerator::createCohortTables(
    connection = connection,
    cohortDatabaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema
  )
  on.exit({DatabaseConnector::dropEmulatedTempTables(connection); DatabaseConnector::disconnect(connection)})

  # test params
  sourcePersonToPersonId <- helper_getParedSourcePersonAndPersonIds(
    connection = connection,
    cohortDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    numberPersons = 2*5
  )

  cohort_data <- tibble::tibble(
    cohort_name = rep(c("Cohort A", "Cohort B"), 5),
    person_source_value = sourcePersonToPersonId$person_source_value,
    cohort_start_date = rep(as.Date(c("2020-01-01", "2020-01-04")), 5),
    cohort_end_date = rep(as.Date(c(NA, "2020-01-04")), 5)
  )

  cohortDefinitionSet <- cohortDataToCohortDefinitionSet(
    cohortData = cohort_data
  )

  # function
  generateCohortSetResults <- CohortGenerator_generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    cohortDefinitionSet = cohortDefinitionSet,
    incremental = TRUE,
    incrementalFolder = tempdir()
  )

  # expectations
  generateCohortSetResults |> tidyr::unnest(extraInfo) |>
    dplyr::select(n_source_person, n_source_entries, n_missing_source_person, n_missing_cohort_start, n_missing_cohort_end) |>
    expect_equal(tibble::tibble(
      n_source_person = c(5, 5),
      n_source_entries = c(5, 5),
      n_missing_source_person = c(0, 0),
      n_missing_cohort_start = c(0, 0),
      n_missing_cohort_end = c(5, 0)
    ))

})
