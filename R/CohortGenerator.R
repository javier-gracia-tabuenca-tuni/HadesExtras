


#' Delete cohort from cohort table
#'
#' This function deletes specified cohorts from the cohort table and cohort names table in the database.
#'
#' @param connectionDetails A list containing the necessary details to establish a database connection.
#' @param schema The name of the schema where the cohort table and cohort names table are located.
#' @param cohort_table_name The name of the cohort table.
#' @param cohort_names A character vector of cohort names to be deleted.
#'
#' @importFrom DatabaseConnector connect disconnect executeSql
#' @importFrom SqlRender readSql render translate
#'
#' @return TRUE if the deletion is successful.
#'
#' @export
#'
CohortGenerator_deleteCohortFromCohortTable  <- function(
    connectionDetails = NULL,
    connection = NULL,
    cohortDatabaseSchema,
    cohortTableNames,
    cohortIds){
  #
  # Validate parameters
  #
  if (is.null(connection) && is.null(connectionDetails)) {
    stop("You must provide either a database connection or the connection details.")
  }

  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  checkmate::assertCharacter(cohortDatabaseSchema, len = 1)
  checkmate::assertList(cohortTableNames)
  checkmate::assertNumber(cohortIds)

  #browser()
  # Function
  sql <- SqlRender::readSql(system.file("sql/sql_server/DeleteCohortFromCohortTables.sql", package = "HadesExtras", mustWork = TRUE))
  sql <- SqlRender::render(
    sql = sql,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTableNames$cohortTable,
    cohort_ids = paste0("(", paste0(cohortIds, collapse = " ,"), ")"),
    warnOnMissingParameters = TRUE
  )
  sql <- SqlRender::translate(
    sql = sql,
    targetDialect = connection@dbms
  )
  DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)

  return(TRUE)
}



