
#
# CohortGenerator_deleteCohortFromCohortTable
#
test_that("CohortGenerator_deleteCohortFromCohortTable deletes a cohort", {

  connection <- helper_createNewConnection()

  CohortGenerator::createCohortTables(
    connection = connection,
    cohortDatabaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema,
    cohortTableNames = getCohortTableNames(testSelectedConfiguration$cohortTable$cohortTableName),
  )

  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
    settingsFileName = here::here("inst/testdata/matching/Cohorts.csv"),
    jsonFolder = here::here("inst/testdata/matching/cohorts"),
    sqlFolder = here::here("inst/testdata/matching/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortName"),
    #packageName = "HadesExtras",
    verbose = FALSE
  )

  generatedCohorts <- CohortGenerator::generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    cohortDatabaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema,
    cohortTableNames = getCohortTableNames(testSelectedConfiguration$cohortTable$cohortTableName),
    cohortDefinitionSet = cohortDefinitionSet,
    incremental = FALSE
  )

  generatedCohorts$cohortId |>  expect_equal(c(10,20))

  resultDelete <- CohortGenerator_deleteCohortFromCohortTable(
    connection = connection,
    cohortDatabaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema,
    cohortTableNames = getCohortTableNames(testSelectedConfiguration$cohortTable$cohortTableName),
    cohortIds = c(10L)
  )

  resultDelete |> expect_true()

  codeCounts <- CohortGenerator::getCohortCounts(
    connection = connection,
    cohortDatabaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema,
    cohortTable = testSelectedConfiguration$cohortTable$cohortTableName
  )

  codeCounts$cohortId |> expect_equal(20)
})


#
# CohortGenerator_generateCohortSet
#
test_that("cohortDataToCohortDefinitionSet works", {
  # get test settings
  connection <- helper_createNewConnection()
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
  cohortGeneratorResults <- CohortGenerator_generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    cohortDefinitionSet = cohortDefinitionSet,
    incremental = FALSE
  )


  # expectations
  cohortGeneratorResults |> checkmate::expect_tibble()
  cohortGeneratorResults |> names() |> checkmate::expect_names(must.include = c("cohortId","generationStatus", "startTime","endTime","buildInfo"))

  cohortGeneratorResults$generationStatus |> expect_equal(c("COMPLETE", "COMPLETE"))

  cohortGeneratorResults$buildInfo[[1]]$logTibble$message |> expect_equal("All person_source_values were found")
  cohortGeneratorResults$buildInfo[[2]]$logTibble$message |> expect_equal("All person_source_values were found")

})

test_that("cohortDataToCohortDefinitionSet reports missing source person id", {
  # get test settings
  connection <- helper_createNewConnection()
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
  cohortGeneratorResults <- CohortGenerator_generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    cohortDefinitionSet = cohortDefinitionSet,
    incremental = FALSE
  )


  # expectations
  cohortGeneratorResults |> checkmate::expect_tibble()
  cohortGeneratorResults |> names() |> checkmate::expect_names(must.include = c("cohortId","generationStatus", "startTime","endTime","buildInfo"))

  cohortGeneratorResults$generationStatus |> expect_equal(c("COMPLETE", "COMPLETE"))

  cohortGeneratorResults$buildInfo[[1]]$logTibble$message |> expect_equal("2 person_source_values were not found")
  cohortGeneratorResults$buildInfo[[2]]$logTibble$message |> expect_equal("3 person_source_values were not found")

})

test_that("cohortDataToCohortDefinitionSet reports missing cohort_start_date", {
  # get test settings
  connection <- helper_createNewConnection()
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
  cohortGeneratorResults <- CohortGenerator_generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    cohortDefinitionSet = cohortDefinitionSet,
    incremental = FALSE
  )


  # expectations
  cohortGeneratorResults |> checkmate::expect_tibble()
  cohortGeneratorResults |> names() |> checkmate::expect_names(must.include = c("cohortId","generationStatus", "startTime","endTime","buildInfo"))

  cohortGeneratorResults$generationStatus |> expect_equal(c("COMPLETE", "COMPLETE"))

  cohortGeneratorResults$buildInfo[[1]]$logTibble$message |> expect_equal("All person_source_values were found")
  cohortGeneratorResults$buildInfo[[1]]$logTibble$message[[1]] |> expect_equal("All person_source_values were found")
  cohortGeneratorResults$buildInfo[[2]]$logTibble$message[[2]] |> expect_equal("5 cohort_start_dates were missing and set to the first observation date")

})

test_that("cohortDataToCohortDefinitionSet reports missing cohort_end_date", {
  # get test settings
  connection <- helper_createNewConnection()
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
  cohortGeneratorResults <- CohortGenerator_generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    cohortDefinitionSet = cohortDefinitionSet,
    incremental = FALSE
  )

  # expectations
  cohortGeneratorResults |> checkmate::expect_tibble()
  cohortGeneratorResults |> names() |> checkmate::expect_names(must.include = c("cohortId","generationStatus", "startTime","endTime","buildInfo"))

  cohortGeneratorResults$generationStatus |> expect_equal(c("COMPLETE", "COMPLETE"))

  cohortGeneratorResults$buildInfo[[1]]$logTibble$message[[1]] |> expect_equal("All person_source_values were found")
  cohortGeneratorResults$buildInfo[[1]]$logTibble$message[[2]] |> expect_equal("5 cohort_end_dates were missing and set to the first observation date")
  cohortGeneratorResults$buildInfo[[2]]$logTibble$message[[1]] |> expect_equal("All person_source_values were found")

})


test_that("cohortDataToCohortDefinitionSet also works from imported files", {
  # get test settings
  connection <- helper_createNewConnection()
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
  cohortGeneratorResults <- CohortGenerator_generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    cohortDefinitionSet = cohortDefinitionSet,
    incremental = FALSE
  )

  # expectations
  cohortGeneratorResults |> checkmate::expect_tibble()
  cohortGeneratorResults |> names() |> checkmate::expect_names(must.include = c("cohortId","generationStatus", "startTime","endTime","buildInfo"))

  cohortGeneratorResults$generationStatus |> expect_equal(c("COMPLETE", "COMPLETE"))

  cohortGeneratorResults$buildInfo[[1]]$logTibble$message[[1]] |> expect_equal("All person_source_values were found")
  cohortGeneratorResults$buildInfo[[1]]$logTibble$message[[2]] |> expect_equal("5 cohort_end_dates were missing and set to the first observation date")
  cohortGeneratorResults$buildInfo[[2]]$logTibble$message[[1]] |> expect_equal("All person_source_values were found")

})


test_that("cohortDataToCohortDefinitionSet incremental mode do not create the tmp cohortData", {
  # get test settings
  connection <- helper_createNewConnection()
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

  incrementalFolder <- file.path(tempdir(),stringr::str_remove_all(Sys.time(),"-|:|\\.|\\s"))
  on.exit({unlink(incrementalFolder, recursive = TRUE)})

  cohortGeneratorResults <- CohortGenerator_generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    cohortDefinitionSet = cohortDefinitionSet,
    incremental = TRUE,
    incrementalFolder = incrementalFolder
  )

  # expectations first run
  cohortGeneratorResults$generationStatus |> expect_equal(c("COMPLETE", "COMPLETE"))
  cohortGeneratorResults$buildInfo[[1]]$logTibble$message |> expect_equal("All person_source_values were found")
  cohortGeneratorResults$buildInfo[[2]]$logTibble$message |> expect_equal("All person_source_values were found")


  cohort_data <- tibble::tibble(
    cohort_name = rep(c("Cohort A", "Cohort B"), 5),
    person_source_value = sourcePersonToPersonId$person_source_value,
    cohort_start_date = rep(as.Date(c("2020-01-01", "2020-01-01")), 5),
    cohort_end_date = rep(as.Date(c(NA, "2020-01-04")), 5)
  )

  cohortDefinitionSet <- cohortDataToCohortDefinitionSet(
    cohortData = cohort_data
  )

  cohortGeneratorResults <- CohortGenerator_generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    cohortDefinitionSet = cohortDefinitionSet,
    incremental = TRUE,
    incrementalFolder = incrementalFolder
  )

  # expectations firt run
  cohortGeneratorResults$generationStatus |> expect_equal(c("COMPLETE", "SKIPPED"))

  cohortGeneratorResults$buildInfo[[1]]$logTibble$message[[1]] |> expect_equal("All person_source_values were found")
  cohortGeneratorResults$buildInfo[[1]]$logTibble$message[[2]] |> expect_equal("5 cohort_end_dates were missing and set to the first observation date")
  cohortGeneratorResults$buildInfo[[2]]$logTibble |> nrow() |> expect_equal(0)

})
