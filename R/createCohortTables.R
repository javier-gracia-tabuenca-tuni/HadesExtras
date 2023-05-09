#' Create cohort tables
#'
#' Creates cohort tables in a database schema if they do not already exist.
#'
#' @param connectionDetails details for connecting to the database
#' @param schema name of the database schema to use
#' @param cohort_table_name name of the cohort table to create
#'
#' @importFrom DatabaseConnector connect existsTable insertTable disconnect
#' @importFrom tibble tibble
#'
#' @return \code{TRUE} if the cohort tables were successfully created, \code{FALSE} otherwise
#' @export

createCohortTables <- function(connectionDetails, schema, cohort_table_name) {


  cohort_names_table_name <- paste0(cohort_table_name, "_names")

  connection <- DatabaseConnector::connect(connectionDetails)

  if(DatabaseConnector::existsTable(connection, schema, cohort_table_name) &
     DatabaseConnector::existsTable(connection, schema, cohort_names_table_name)){
     DatabaseConnector::disconnect(connection)
    return(FALSE)
  }

  cohort_table <- tibble::tibble(
    cohort_definition_id = as.integer(NA),
    subject_id = as.integer(NA),
    cohort_start_date = as.Date(NA),
    cohort_end_date = as.Date(NA),
    .rows = NULL
  )

  DatabaseConnector::insertTable(connection, schema, cohort_table_name, cohort_table)

  cohort_names_table <- tibble::tibble(
    cohort_definition_id = as.integer(NA),
    cohort_name = as.character(NA),
    atlas_cohort_definition_id = as.integer(NA),
    .rows = NULL
  )

  DatabaseConnector::insertTable(connection, schema, cohort_names_table_name, cohort_names_table)

  connection <- DatabaseConnector::connect(connectionDetails)

  return(TRUE)

}
