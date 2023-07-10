
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
test_that("cohortDataToCohortDefinitionSet works", {
  # get test settings
  connection <- helper_getConnectionToTestConfiguration()
  on.exit({DatabaseConnector::dropEmulatedTempTables(connection); DatabaseConnector::disconnect(connection)})
  testSelectedConfiguration  <- getOption("testSelectedConfiguration")
  cohortDatabaseSchema <- testSelectedConfiguration$cdm$cdmDatabaseSchema

  # test params
  sourcePersonToPersonId <- helper_getParedSourcePersonAndPersonIds(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    numberPersons = 2*5
  )

  cohort_data <- tibble::tibble(
    cohort_name = rep(c("Cohort A", "Cohort B"), 5),
    person_source_value = sourcePersonToPersonId$person_source_value,
    cohort_start_date = rep(as.Date(c("2020-01-01", "2020-01-01")), 5),
    cohort_end_date = rep(as.Date(c("2020-01-03", "2020-01-04")), 5)
  )


  # function
  cohortDefinitionSet <- cohortDataToCohortDefinitionSet(
    connection = connection,
    cdmDatabaseSchema = cohortDatabaseSchema,
    cohortData = cohort_data
  )

  # expectations
  cohortDefinitionSet |> CohortGenerator::isCohortDefinitionSet() |> expect_true()

  cohortDefinitionSet |> tidyr::unnest(extra_info) |>
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
  connection <- helper_getConnectionToTestConfiguration()
  on.exit({DatabaseConnector::dropEmulatedTempTables(connection); DatabaseConnector::disconnect(connection)})
  testSelectedConfiguration  <- getOption("testSelectedConfiguration")
  cohortDatabaseSchema <- testSelectedConfiguration$cdm$cdmDatabaseSchema

  # test params
  sourcePersonToPersonId <- helper_getParedSourcePersonAndPersonIds(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    numberPersons = 2*5
  )

  cohort_data <- tibble::tibble(
    cohort_name = rep(c("Cohort A", "Cohort B"), 5),
    person_source_value = c(sourcePersonToPersonId$person_source_value[1:5], letters[1:5]),
    cohort_start_date = rep(as.Date(c("2020-01-01", "2020-01-01")), 5),
    cohort_end_date = rep(as.Date(c("2020-01-03", "2020-01-04")), 5)
  )

  # function
  cohortDefinitionSet <- cohortDataToCohortDefinitionSet(
    connection = connection,
    cdmDatabaseSchema = cohortDatabaseSchema,
    cohortData = cohort_data
  )

  # expectations
  cohortDefinitionSet |> CohortGenerator::isCohortDefinitionSet() |> expect_true()

  cohortDefinitionSet |> tidyr::unnest(extra_info) |>
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
  connection <- helper_getConnectionToTestConfiguration()
  on.exit({DatabaseConnector::dropEmulatedTempTables(connection); DatabaseConnector::disconnect(connection)})
  testSelectedConfiguration  <- getOption("testSelectedConfiguration")
  cohortDatabaseSchema <- testSelectedConfiguration$cdm$cdmDatabaseSchema

  # test params
  sourcePersonToPersonId <- helper_getParedSourcePersonAndPersonIds(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    numberPersons = 2*5
  )

  cohort_data <- tibble::tibble(
    cohort_name = rep(c("Cohort A", "Cohort B"), 5),
    person_source_value = sourcePersonToPersonId$person_source_value,
    cohort_start_date = rep(as.Date(c("2020-01-01", NA)), 5),
    cohort_end_date = rep(as.Date(c("2020-01-03", "2020-01-04")), 5)
  )

  # function
  cohortDefinitionSet <- cohortDataToCohortDefinitionSet(
    connection = connection,
    cdmDatabaseSchema = cohortDatabaseSchema,
    cohortData = cohort_data
  )

  # expectations
  cohortDefinitionSet |> CohortGenerator::isCohortDefinitionSet() |> expect_true()

  cohortDefinitionSet |> tidyr::unnest(extra_info) |>
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
  connection <- helper_getConnectionToTestConfiguration()
  on.exit({DatabaseConnector::dropEmulatedTempTables(connection); DatabaseConnector::disconnect(connection)})
  testSelectedConfiguration  <- getOption("testSelectedConfiguration")
  cohortDatabaseSchema <- testSelectedConfiguration$cdm$cdmDatabaseSchema

  # test params
  sourcePersonToPersonId <- helper_getParedSourcePersonAndPersonIds(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    numberPersons = 2*5
  )

  cohort_data <- tibble::tibble(
    cohort_name = rep(c("Cohort A", "Cohort B"), 5),
    person_source_value = sourcePersonToPersonId$person_source_value,
    cohort_start_date = rep(as.Date(c("2020-01-01", "2020-01-04")), 5),
    cohort_end_date = rep(as.Date(c(NA, "2020-01-04")), 5)
  )

  # function
  cohortDefinitionSet <- cohortDataToCohortDefinitionSet(
    connection = connection,
    cdmDatabaseSchema = cohortDatabaseSchema,
    cohortData = cohort_data
  )

  # expectations
  cohortDefinitionSet |> CohortGenerator::isCohortDefinitionSet() |> expect_true()

  cohortDefinitionSet |> tidyr::unnest(extra_info) |>
    dplyr::select(n_source_person, n_source_entries, n_missing_source_person, n_missing_cohort_start, n_missing_cohort_end) |>
    expect_equal(tibble::tibble(
      n_source_person = c(5, 5),
      n_source_entries = c(5, 5),
      n_missing_source_person = c(0, 0),
      n_missing_cohort_start = c(0, 0),
      n_missing_cohort_end = c(5, 0)
    ))

})


