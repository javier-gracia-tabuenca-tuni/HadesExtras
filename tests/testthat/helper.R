


helper_getDatabaseSettings <- function(change_cohort_table = NULL){
  database_settings <- readDatabaseSettings(
    path_databases_settings_yalm = testthat::test_path("config", "test_config.yml"),
    database_name = getOption("test_database_settings_name", default = "dev_eunomia"))

  if(!is.null(change_cohort_table)){
    database_settings$tables$cohort_table <- change_cohort_table
    database_settings$tables$cohort_names_table <- paste0(change_cohort_table, "_names")
  }

  return(database_settings)
}



helper_createCohortTableWithTestData <- function(
    database_settings,
    cohort_names,
    n_persons = 5,
    base_start_date = "2000-01-01",
    base_end_date = "2020-01-01"
    ){

  # get settings
  n_cohorts <- length(cohort_names)

  # create tables, drop if exists
  createCohortTables(database_settings)

  # Connect, collect tables
  connection <- DatabaseConnector::connect(database_settings$connectionDetails)

  # writes data to cohort_names_table
  test_cohort_names_table <- tibble::tibble(
    cohort_definition_id = 1:n_cohorts,
    cohort_name = cohort_names,
    atlas_cohort_definition_id = as.integer(rep(NA, n_cohorts))
  )

  DatabaseConnector::dbWriteTable(connection, database_settings$tables$cohort_names_table, test_cohort_names_table, overwrite = TRUE)

  # writes data to cohort_table
  test_cohort_table <- tibble::tibble(
    cohort_definition_id = rep(1:n_cohorts,n_persons) |> sort(),
    subject_id = 1:(n_cohorts*n_persons),
    cohort_start_date = as.Date(rep(base_start_date, n_cohorts*n_persons))+subject_id,
    cohort_end_date = as.Date(rep(base_end_date, n_cohorts*n_persons))+subject_id
  )

  DatabaseConnector::dbWriteTable(connection, database_settings$tables$cohort_table, test_cohort_table, overwrite = TRUE)

  DatabaseConnector::disconnect(connection)
}


helper_getSourcePersonIdFromPersonId  <- function(database_settings, person_ids){

  # Connect, collect tables
  connection <- DatabaseConnector::connect(database_settings$connectionDetails)
  person_table <- dplyr::tbl(connection, tmp_inDatabaseSchema(database_settings$schemas$CDM, "person"))

  # get first n persons
  source_person_ids  <- person_table  |> dplyr::filter(person_id %in% person_ids) |>
    dplyr::arrange(person_id) |>
    dplyr::pull(person_source_value)

  #disconnect
  DatabaseConnector::disconnect(connection)

  return(source_person_ids)
}





























