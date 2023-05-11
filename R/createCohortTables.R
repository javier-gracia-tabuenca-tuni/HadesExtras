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

 sql <- SqlRender::readSql(system.file("sql/sql_server/CreateCohortTables.sql", package = "HadesExtras", mustWork = TRUE))
    sql <- SqlRender::render(
      sql = sql,
      cohort_database_schema = schema,
      cohort_table = cohort_table_name,
      cohort_name_table = cohort_names_table_name,
      warnOnMissingParameters = TRUE
    )
    sql <- SqlRender::translate(
      sql = sql,
      targetDialect = connection@dbms
    )
    DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)

    DatabaseConnector::disconnect(connection)

  return(TRUE)

}
