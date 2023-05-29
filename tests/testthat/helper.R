


helper_getDatabaseSettings <- function(){
  database_settings <- readDatabaseSettings(
    path_databases_settings_yalm = testthat::test_path("config", "test_config.yml"),
    database_name = getOption("test_database_settings_name", default = "dev_bigquery")
  )
  return(database_settings)
}



helper_createCohortTableWithTestData <- function(
    connection,
    cohortDatabaseSchema,
    cohortTableNames = getCohortTableNames(),
    testCohortNames,
    numberPersonsPerCohort = 5,
    baseStartDate = "2000-01-01",
    baseEndDate = "2020-01-01"
    ){

  numberCohorts <- length(testCohortNames)

  # create tables, drop if exists
  createCohortTables(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = cohortTableNames
  )

  # create test data
  testCohortTable <- tibble::tibble(
    cohort_definition_id = rep(1:numberCohorts, numberPersonsPerCohort) |> sort(),
    subject_id = 1:( numberCohorts * numberPersonsPerCohort ),
    cohort_start_date = as.Date(rep(baseStartDate, numberCohorts * numberPersonsPerCohort )) + subject_id,
    cohort_end_date = as.Date(rep(baseEndDate, numberCohorts * numberPersonsPerCohort )) + subject_id
  )

  testCohortInfoTable <- tibble::tibble(
    cohort_definition_id = 1:numberCohorts,
    cohort_name = testCohortNames,
    atlas_cohort_definition_id = as.integer(rep(NA, numberCohorts))
  )

  # append test data to tables
  dplyr::rows_append(
    dplyr::tbl(connection, tmp_inDatabaseSchema(cohortDatabaseSchema, cohortTableNames$cohortTable)),
    dplyr::copy_to(connection, testCohortTable),
    in_place = TRUE
  )

  dplyr::rows_append(
    dplyr::tbl(connection, tmp_inDatabaseSchema(cohortDatabaseSchema, cohortTableNames$cohortInfoTable)),
    dplyr::copy_to(connection,testCohortInfoTable),
    in_place = TRUE
  )

}


helper_getParedSourcePersonAndPersonIds  <- function(connection,
                                                     cohortDatabaseSchema,
                                                     numberPersons){

  # Connect, collect tables
  personTable <- dplyr::tbl(connection, tmp_inDatabaseSchema(cohortDatabaseSchema, "person"))

  # get first n persons
  pairedSourcePersonAndPersonIds  <- personTable  |>
    dplyr::arrange(person_id) |>
    dplyr::select(person_id, person_source_value) |>
    dplyr::collect(n=numberPersons)


  return(pairedSourcePersonAndPersonIds)
}





























