
#
# checkCohortData
#

test_that("checkCohortData works", {

  cohortData <- tibble::tibble(
    cohort_name = rep(c("Cohort A", "Cohort B"), 5),
    person_source_value = letters[1:10],
    cohort_start_date = rep(as.Date(c("2020-01-01", "2020-01-01")), 5),
    cohort_end_date = rep(as.Date(c("2020-01-03", "2020-01-04")), 5)
  )


  cohortData |> assertCohortData()
  cohortData |> checkCohortData() |> expect_true()

})

test_that("checkCohortData fails with missing column", {

  cohortData <- tibble::tibble(
    cohort_name = rep(c("Cohort A", "Cohort B"), 5),
    person_source_value = letters[1:10],
    cohort_start_date = rep(as.Date(c("2020-01-01", "2020-01-01")), 5),
    cohort_end_date = rep(as.Date(c("2020-01-03", "2020-01-04")), 5)
  ) |>
    select(-cohort_end_date)

  cohortData |> checkCohortData() |> expect_match("cohort_end_date")

})

test_that("checkCohortData fails with wrong type", {

  cohortData <- tibble::tibble(
    cohort_name = rep(c("Cohort A", "Cohort B"), 5),
    person_source_value = letters[1:10],
    cohort_start_date = rep(as.Date(c("2020-01-01", "2020-01-01")), 5),
    cohort_end_date = rep(as.Date(c("2020-01-03", "2020-01-04")), 5)
  ) |>
    mutate(cohort_end_date = as.character(cohort_end_date))

  cohortData |> checkCohortData() |> expect_match("cohort_end_date is not of type date")

})

test_that("checkCohortData fails with missing values in cohort_name", {

  cohortData <- tibble::tibble(
    cohort_name = rep(c(as.character(NA), "Cohort B"), 5),#changed name
    person_source_value = letters[1:10],
    cohort_start_date = rep(as.Date(c("2020-01-01", "2020-01-01")), 5),
    cohort_end_date = rep(as.Date(c("2020-01-03", "2020-01-04")), 5)
  )

  cohortData |> checkCohortData() |> expect_match("rows are missing cohort_name")

})

test_that("checkCohortData fails with cohort_start_date > cohort_end_date", {

  cohortData <- tibble::tibble(
    cohort_name = rep(c("Cohort A", "Cohort B"), 5),
    person_source_value = letters[1:10],
    cohort_start_date = rep(as.Date(c("2020-01-01", "2022-01-01")), 5), #changed dates second value
    cohort_end_date = rep(as.Date(c("2020-01-03", "2020-01-04")), 5)
  )

  cohortData |> checkCohortData() |> expect_match("rows have cohort_start_date older than cohort_end_date")

})



#
# cohortDataToCohortDefinitionSet
#

test_that(" cohortDataToCohortDefinitionSet works", {

  cohortData <- tibble::tibble(
    cohort_name = rep(c("Cohort A", "Cohort B"), 5),
    person_source_value = letters[1:10],
    cohort_start_date = rep(as.Date(c("2020-01-01", "2020-01-01")), 5),
    cohort_end_date = rep(as.Date(c("2020-01-03", "2020-01-04")), 5)
  )

  cohortDefinitionSet <- cohortDataToCohortDefinitionSet(cohortData)

  cohortDefinitionSet |> checkmate::expect_tibble()
  cohortDefinitionSet |> names() |> checkmate::expect_names(must.include = c("cohortId", "cohortName", "json", "sql" ))
  cohortDefinitionSet |> pull(json) |> stringr::str_detect('cohortType\": \"FromCohortData\"') |> all() |> expect_true()
  (cohortDefinitionSet |> pull(sql) |> unique() |> length() == cohortDefinitionSet |> nrow()) |> expect_true()
  cohortDefinitionSet$sql[[1]] |> stringr::str_detect("WHERE cohort_definition_id = 1") |> expect_true()
  cohortDefinitionSet$sql[[2]] |> stringr::str_detect("WHERE cohort_definition_id = 2") |> expect_true()

})



#
# getCohortDataFromCohortTable
#

test_that("getCohortDataFromCohortTable returns a cohort", {

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

  cohortData <- getCohortDataFromCohortTable(
    connection = connection,
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    cohortDatabaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema,
    cohortTable = testSelectedConfiguration$cohortTable$cohortTableName,
    cohortNameIds = generatedCohorts |> dplyr::select(cohortId, cohortName)
  )

  cohortData |> checkCohortData() |> expect_true()
  cohortData |> nrow() |> expect_gt(0)
})




















