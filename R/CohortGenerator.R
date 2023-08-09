

CohortGenerator_generateCohortSet <- function(
    connectionDetails = NULL,
    connection = NULL,
    cdmDatabaseSchema,
    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
    cohortDatabaseSchema = cdmDatabaseSchema,
    cohortTableNames = getCohortTableNames(),
    cohortDefinitionSet = NULL,
    stopOnError = TRUE,
    incremental = FALSE,
    incrementalFolder = NULL) {

  #
  # Validate parameters
  #
  checkmate::assertDataFrame(cohortDefinitionSet, min.rows = 1, col.names = "named")
  checkmate::assertNames(colnames(cohortDefinitionSet),
                         must.include = c(
                           "cohortId",
                           "cohortName",
                           "sql"
                         )
  )

  if (is.null(connection) && is.null(connectionDetails)) {
    stop("You must provide either a database connection or the connection details.")
  }

  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  #
  # start function before generateCohortSet
  #

  # get cohortType from json
  cohortDefinitionSet <- cohortDefinitionSet |>
    dplyr::mutate(cohortType = purrr::map_chr(.x = json, .f=~{
      l <- RJSONIO::fromJSON(.x)
      if ("cohortType" %in% names(l)) {
        return(l[["cohortType"]])
      }else{
        return(as.character(NA))
      }
    }))

  cohortDefinitionSetCohortDataType <- cohortDefinitionSet |>
    dplyr::filter(cohortType == "FromCohortData")


  if(nrow(cohortDefinitionSetCohortDataType)!=0){
    # separate sql from cohortData
    cohortData <- cohortDefinitionSetCohortDataType |>
      dplyr::mutate(
        data = purrr::map(.x=json, .f = ~{
          l <- RJSONIO::fromJSON(.x, nullValue = as.character(NA), simplify = TRUE)
          l$cohortData |>
            tibble::as_tibble() |>
            dplyr::mutate(
              cohort_start_date = as.Date(cohort_start_date),
              cohort_end_date = as.Date(cohort_end_date)
            )
        })
      ) |>
      dplyr::select(cohortId, data) |>
      tidyr::unnest(data) |>
      dplyr::rename(cohort_definition_id = cohortId)

    # Connect to tables and copy cohortData to database
    personTable <- dplyr::tbl(connection, tmp_inDatabaseSchema(cdmDatabaseSchema, "person"))
    observationPeriodTable <- dplyr::tbl(connection, tmp_inDatabaseSchema(cdmDatabaseSchema, "observation_period"))
    cohortDataTable <- tmp_dplyr_copy_to(connection, cohortData, overwrite = TRUE)

    # join to cohort_data_table cohort_names_table.cohort_name; person.person_id; observation_period period dates
    toAppend <- cohortDataTable |>
      dplyr::left_join(
        personTable |> dplyr::select(person_id, person_source_value),
        by = "person_source_value"
      ) |>
      dplyr::left_join(
        observationPeriodTable |>
          dplyr::select(person_id, observation_period_start_date, observation_period_end_date) |>
          dplyr::group_by(person_id) |>
          dplyr::summarise(
            observation_period_start_date = min(observation_period_start_date, na.rm = TRUE),
            observation_period_end_date = max(observation_period_end_date, na.rm = TRUE)
          ),
        by = "person_id"
      )

    # collect check cohortData to add
    checkOnCohortData <- toAppend |>
      dplyr::group_by(cohort_definition_id) |>
      dplyr::summarise(
        n_source_person = dplyr::n_distinct(person_source_value),
        n_source_entries = dplyr::n(),
        n_missing_source_person = dplyr::n_distinct(person_source_value) - dplyr::n_distinct(person_id),
        n_missing_cohort_start = sum(ifelse(is.null(cohort_start_date),1L,0L), na.rm = TRUE),
        n_missing_cohort_end = sum(ifelse(is.null(cohort_end_date),1L,0L), na.rm = TRUE)
      ) |>
      dplyr::collect()

    # compute changes in toAppend
    toAppend <- toAppend |>
      dplyr::filter(!is.na(person_id)) |>
      # if cohort_start_date and cohort_end_date are na, use observation_period_start_date and observation_period_end_date
      dplyr::mutate(
        cohort_start_date = dplyr::if_else(is.na(cohort_start_date), observation_period_start_date, cohort_start_date),
        cohort_end_date = dplyr::if_else(is.na(cohort_end_date), observation_period_end_date, cohort_end_date)
      ) |>
      dplyr::select(cohort_definition_id, subject_id=person_id, cohort_start_date, cohort_end_date) #|>
      #dplyr::compute()
    # instead of compute, we create a temporal table, so the same stays the same in the sql, this is necesary for incremental mode
    # overwrite not working

    browser()
    DatabaseConnector::dbRemoveTable(connection, "temp_tbl")
    toAppend <- dplyr::copy_to(connection, toAppend, "temp_tbl", temporary = TRUE, overwrite = TRUE)


    sqlToRender <- SqlRender::readSql(system.file("sql/sql_server/ImportCohortTable.sql", package = "HadesExtras", mustWork = TRUE))
    tableNameToRender <- dbplyr::remote_name(toAppend)


    cohortDefinitionSetCohortDataType <- cohortDefinitionSetCohortDataType |>
      dplyr::mutate(
        sql = purrr::map2_chr(
          .x = cohortId,
          .y = sql,
          .f=~{paste(.y, "\n",
                     SqlRender::render(
                       sql = sqlToRender,
                       source_cohort_table = tableNameToRender,
                       source_cohort_id = .x,
                       is_temp_table = TRUE
                     ))}
        ),
        json = json
      )


    # replace recalculated cohortDefinitionSets
    cohortDefinitionSet <- dplyr::bind_rows(
      cohortDefinitionSet |> dplyr::filter(cohortType != "FromCohortData"),
      cohortDefinitionSetCohortDataType
    )
  }

  #
  # end function before generateCohortSet
  #
  results <- CohortGenerator::generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortDefinitionSet,
    stopOnError = stopOnError,
    incremental = incremental,
    incrementalFolder = incrementalFolder
  )

  #
  # start function after generateCohortSet
  #
  if(nrow(cohortDefinitionSetCohortDataType)!=0){
    results <- results |>
      dplyr::left_join(checkOnCohortData, by = c("cohortId" = "cohort_definition_id")) |>
      tidyr::nest(.key = "extraInfo", .by = c("cohortName", "cohortId", "generationStatus", "startTime", "endTime"))

    DatabaseConnector::dropEmulatedTempTables(connection)
  }

  #
  # end function after generateCohortSet
  #

  return(results)
}









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
  checkmate::assertNumeric(cohortIds)

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


#' Get cohort counts and basic cohort demographics for a specific cohort.
#'
#' This function retrieves demographic information for a given cohort .
#'
#' @param connectionDetails Details required to establish a database connection (optional).
#' @param connection An existing database connection (optional).
#' @param cdmDatabaseSchema The schema name for the Common Data Model (CDM) database.
#' @param vocabularyDatabaseSchema The schema name for the vocabulary database (default is \code{cdmDatabaseSchema}).
#' @param cohortDatabaseSchema The schema name for the cohort database.
#' @param cohortTable The name of the cohort table in the cohort database (default is "cohort").
#' @param cohortIds A numeric vector of cohort IDs for which to retrieve demographics (default is an empty vector).
#' @param toGet A character vector indicating which demographic information to retrieve. Possible values include "histogramCohortStartYear", "histogramCohortEndYear", "histogramBirthYear", and "sexCounts".
#' @param cohortDefinitionSet A set of cohort definitions (optional).
#' @param databaseId The ID of the database (optional).
#'
#' @return
#' A data frame with cohort counts and selected demographics
#'
#' @importFrom DatabaseConnector connect disconnect getTableNames
#' @importFrom dplyr tbl count collect mutate left_join distinct select nest_by
#' @importFrom CohortGenerator getCohortCounts
#'
#' @export
CohortGenerator_getCohortDemograpics <- function(
    connectionDetails = NULL,
    connection = NULL,
    cdmDatabaseSchema,
    vocabularyDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema,
    cohortTable = "cohort",
    cohortIds = c(),
    toGet = c("histogramCohortStartYear", "histogramCohortEndYear", "histogramBirthYear", "sexCounts"),
    cohortDefinitionSet = NULL,
    databaseId = NULL
) {

  start <- Sys.time()

  #
  # validate parameters
  #
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  tablesInServer <- tolower(DatabaseConnector::getTableNames(conn = connection, databaseSchema = cohortDatabaseSchema))
  if (!(tolower(cohortTable) %in% tablesInServer)) {
    warning("Cohort table was not found. Was it created?")
    return(NULL)
  }

  toGet |> checkmate::assertSubset(c( "histogramCohortStartYear", "histogramCohortEndYear", "histogramBirthYear", "sexCounts"))

  #
  # function
  #
  cohortTable <- dplyr::tbl(connection, tmp_inDatabaseSchema(cohortDatabaseSchema, cohortTable))
  personTable  <- dplyr::tbl(connection, tmp_inDatabaseSchema(cdmDatabaseSchema, "person"))
  conceptTable  <- dplyr::tbl(connection, tmp_inDatabaseSchema(vocabularyDatabaseSchema, "concept"))

  cohortCounts <- CohortGenerator::getCohortCounts(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortIds = cohortIds,
    cohortDefinitionSet = cohortDefinitionSet,
    databaseId = databaseId
  )

  histogramCohortStartYear <-  tibble::tibble(cohort_definition_id=0, .rows = 0)
  if ("histogramCohortStartYear" %in% toGet) {
    histogramCohortStartYear <- cohortTable |>
      dplyr::mutate( year = year(cohort_start_date) ) |>
      dplyr::count(cohort_definition_id, year)  |>
      dplyr::collect() |>
      dplyr::nest_by(cohort_definition_id, .key = "histogramCohortStartYear")
  }

  histogramCohortEndYear <-  tibble::tibble(cohort_definition_id=0, .rows = 0)
  if ("histogramCohortEndYear" %in% toGet) {
    histogramCohortEndYear <- cohortTable |>
      dplyr::mutate( year = year(cohort_end_date) ) |>
      dplyr::count(cohort_definition_id, year)  |>
      dplyr::collect() |>
      dplyr::nest_by(cohort_definition_id, .key = "histogramCohortEndYear")
  }

  histogramBirthYear <-  tibble::tibble(cohort_definition_id=0, .rows = 0)
  if ("histogramBirthYear" %in% toGet) {
    histogramBirthYear <- cohortTable |>
      dplyr::distinct(subject_id) |>
      dplyr::left_join(
        personTable  |> dplyr::select(person_id, year_of_birth),
        by = c("subject_id" = "person_id")
      )|>
      dplyr::mutate( year = year_of_birth ) |>
      dplyr::count(cohort_definition_id, year)  |>
      dplyr::collect() |>
      dplyr::nest_by(cohort_definition_id, .key = "histogramBirthYear")
  }

  sexCounts <-  tibble::tibble(cohort_definition_id=0, .rows = 0)
  if ("sexCounts" %in% toGet) {
    sexCounts <- cohortTable  |>
      dplyr::left_join(
        personTable  |> dplyr::select(person_id, gender_concept_id),
        by = c("subject_id" = "person_id")
      ) |>
      dplyr::left_join(
        conceptTable |> dplyr::select(concept_id, concept_name),
        by = c("gender_concept_id" = "concept_id")
      ) |>
      dplyr::count(cohort_definition_id, sex=concept_name)|>
      dplyr::collect() |>
      dplyr::nest_by(cohort_definition_id, .key = "count_sex")
  }

  cohortsSummary <- cohortCounts |>
    dplyr::left_join(histogramCohortStartYear, by = c("cohortId" = "cohort_definition_id")) |>
    dplyr::left_join(histogramCohortEndYear, by = c("cohortId" = "cohort_definition_id")) |>
    dplyr::left_join(histogramBirthYear, by = c("cohortId" = "cohort_definition_id")) |>
    dplyr::left_join(sexCounts, by = c("cohortId" = "cohort_definition_id"))


  ParallelLogger::logInfo(paste("Counting cohorts took", signif(delta, 3), attr(delta, "units")))

  return(cohortDemograpics)

}
