


CohortGenerator_generateCohortDefinitionSetToImportFromExternalCohortTable <- function(
      externalCohortDatabaseSchema,
      externalCohortTableName,
      externalCohortIds,
      externalCohortNames,
      offsetCohortId = 0,
      isFromAtlasCohortTable = FALSE
){

  checkmate::assertCharacter(externalCohortDatabaseSchema, len = 1)
  checkmate::assertCharacter(externalCohortTableName, len = 1)
  checkmate::assertInteger(externalCohortIds, )
  checkmate::assertCharacter(externalCohortNames)
  checkmate::assertInt(offsetCohortId)
  checkmate::assertLogical(isFromAtlasCohortTable)


  cohortDefinitionSet <- CohortGenerator::createEmptyCohortDefinitionSet() |> tibble::as_tibble()

  for(i in 1:length(externalCohortIds)){
    cohortId <- externalCohortIds[i]
    cohortName <- externalCohortNames[i]

    # Function
    sql <- SqlRender::readSql(system.file("sql/sql_server/ImportCohortTable.sql", package = "HadesExtras", mustWork = TRUE))
    sql <- SqlRender::render(
      sql = sql,
      external_cohort_database_schema = externalCohortDatabaseSchema,
      external_cohort_table_name = externalCohortTableName,
      external_cohort_id = cohortId,
      warnOnMissingParameters = FALSE
    )

    cohortDefinitionSet <- dplyr::bind_rows(
      cohortDefinitionSet,
      tibble::tibble(
        cohortId = cohortId + offsetCohortId,
        cohortName = cohortName,
        sql = sql,
        json = ""
      )
    )
  }

  return(cohortDefinitionSet)
}




appendCohortDataToCohortTable <- function(connectionDetails = NULL,
                                          connection = NULL,
                                          cohortDatabaseSchema,
                                          cdmDatabaseSchema,
                                          cohortTableNames = getCohortTableNames(),
                                          cohortData){

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

  cohortTableCohortNames <- getCohortNamesFromCohortTable(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = cohortTableNames
  )
  cohortDataCohortNames <- cohortData |> dplyr::distinct(cohort_name) |> dplyr::pull()

  namesExistInCohortTable <- intersect(cohortTableCohortNames, cohortDataCohortNames)
  if (length(namesExistInCohortTable) !=0 ) {
    stop("Cohorts already exists with these names ", paste(namesExistInCohortTable, collapse = ", "))
  }

  log <- logTibble_NewLog()

  #
  # Connect to tables
  #
  cohortTable <- dplyr::tbl(connection, tmp_inDatabaseSchema(cohortDatabaseSchema, cohortTableNames$cohortTable))
  cohortInfoTable <- dplyr::tbl(connection, tmp_inDatabaseSchema(cohortDatabaseSchema, cohortTableNames$cohortInfoTable))
  personTable <- dplyr::tbl(connection, tmp_inDatabaseSchema(cdmDatabaseSchema, "person"))
  observationPeriodTable <- dplyr::tbl(connection, tmp_inDatabaseSchema(cdmDatabaseSchema, "observation_period"))

  # get safely max cohort_definition_id in cohort_table
  maxCohortDefinitionId <- cohortInfoTable |>
    dplyr::pull(cohort_definition_id) |> max()
  maxCohortDefinitionId <- ifelse(is.na(maxCohortDefinitionId), 0, maxCohortDefinitionId)

  # add a new cohort_definition_id to each cohort name in cohort_data
  cohortData <- cohortData |>
    tidyr::nest(.by = "cohort_name") |>
    dplyr::mutate(cohort_definition_id = maxCohortDefinitionId + dplyr::row_number()) |>
    tidyr::unnest(data)

  # copy table to database
  cohortDataTable <- dplyr::copy_to(connection, cohortData)

  # join to cohort_data_table cohort_names_table.cohort_name; person.person_id; observation_period period dates
  toAppend <- cohortDataTable |>
    dplyr::inner_join(
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

  # log checks
  messageNumberOfPersonsAddedPerCohort <-
    cohortDataTable |> dplyr::count(cohort_name, name="n_cohort_data") |>
    dplyr::left_join(
      toAppend |> dplyr::count(cohort_name, name="n_cohort_table")
    ) |>
    dplyr::mutate(n_diff = n_cohort_data - n_cohort_table) |>
    dplyr::filter(is.na(n_diff)|n_diff!=0) |>
    dplyr::collect() |>
    dplyr::mutate(n_patients = dplyr::if_else(
      is.na(n_diff),
      paste0("Cohort '", cohort_name, "' has not been created because none of the source_patient_id were not found in the database"),
      paste0("For cohort '", cohort_name, "' ", n_diff, " source_patient_id were not found in the database")
    )) |>
    dplyr::pull(n_patients) |>
    as.character()

  log <- logTibble_WARNING(log, "source_person_id not found in database", messageNumberOfPersonsAddedPerCohort)

  messageFixesNAsInCohortStarDatePerCohort  <- toAppend  |>
    dplyr::count(cohort_name, is_na=is.na(cohort_start_date)) |>
    dplyr::filter(is_na!=0) |>
    dplyr::collect() |>
    dplyr::mutate(n_cohort_start_date_na = paste0("For cohort '", cohort_name, "' ",n, " cohort_start_dates are NA and were replaced by observation_period_start_date")) |>
    dplyr::pull(n_cohort_start_date_na) |> as.character()

  log <- logTibble_WARNING(log, "Filled in cohort_start_date", messageFixesNAsInCohortStarDatePerCohort)

  messageFixesNAsInCohortENDDatePerCohort  <- toAppend  |>
    dplyr::count(cohort_name, is_na=is.na(cohort_end_date)) |>
    dplyr::filter(is_na!=0) |>
    dplyr::collect() |>
    dplyr::mutate(n_cohort_end_date_na = paste0("For cohort '", cohort_name, "' ",n, " cohort_end_dates are NA and were replaced by observation_period_end_date")) |>
    dplyr::pull(n_cohort_end_date_na) |> as.character()

  log <- logTibble_WARNING(log, "Filled in cohort_start_date", messageFixesNAsInCohortENDDatePerCohort)

  # append to cohort_table
  dplyr::rows_append(
    cohortTable,
    toAppend |>
      # if cohort_start_date and cohort_end_date are na, use observation_period_start_date and observation_period_end_date
      dplyr::mutate(
        cohort_start_date = dplyr::if_else(is.na(cohort_start_date), observation_period_start_date, cohort_start_date),
        cohort_end_date = dplyr::if_else(is.na(cohort_end_date), observation_period_end_date, cohort_end_date)
      ) |>
      dplyr::select(cohort_definition_id, subject_id=person_id, cohort_start_date, cohort_end_date),
    in_place = TRUE
  )

  # append to cohort_names_table
  dplyr::rows_append(
    cohortInfoTable,
    toAppend |>
      dplyr::distinct(cohort_definition_id, cohort_name) |>
      dplyr::mutate(atlas_cohort_definition_id = as.integer(NA)),
    in_place = TRUE
  )

  if(nrow(log)==0){
    messageNumberOfPersonsAddedPerCohort <- cohortData  |>
      dplyr::count(cohort_name)  |>
      dplyr::mutate(n_patients = paste0("For cohort '", cohort_name, "' ", n, " patients were added to the cohort table"))  |>
      dplyr::pull(n_patients)

    log <- logTibble_INFO(log, "Cohort table filled in successfully", messageNumberOfPersonsAddedPerCohort)
  }

  return(log)
}



getCohortsSummary  <- function(connectionDetails = NULL,
                                   connection = NULL,
                                   cohortDatabaseSchema,
                                   cdmDatabaseSchema,
                                   vocabularyDatabaseSchema,
                                   cohortTableNames = getCohortTableNames()){

  #
  # Validate parameters
  #
  if (is.null(connection) && is.null(connectionDetails)) {
    stop("You must provide either a database connection or the connection details.")
  }

  start <- Sys.time()
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  #
  # Connect to tables
  #
  cohortTable <- dplyr::tbl(connection, tmp_inDatabaseSchema(cohortDatabaseSchema, cohortTableNames$cohortTable))
  cohortInfoTable <- dplyr::tbl(connection, tmp_inDatabaseSchema(cohortDatabaseSchema, cohortTableNames$cohortInfoTable))
  personTable  <- dplyr::tbl(connection, tmp_inDatabaseSchema(cdmDatabaseSchema, "person"))
  conceptTable  <- dplyr::tbl(connection, tmp_inDatabaseSchema(vocabularyDatabaseSchema, "concept"))

  #
  # Function
  #
  histogramCohortStartYear <- cohortTable |>
    dplyr::mutate( cohort_start_year = year(cohort_start_date) ) |>
    dplyr::count(cohort_definition_id, cohort_start_year)  |>
    dplyr::collect() |>
    dplyr::nest_by(cohort_definition_id, .key = "histogram_cohort_start_year")

  histogramCohortEndYear <- cohortTable |>
    dplyr::mutate( cohort_end_year = year(cohort_end_date) ) |>
    dplyr::count(cohort_definition_id, cohort_end_year)  |>
    dplyr::collect() |>
    dplyr::nest_by(cohort_definition_id, .key = "histogram_cohort_end_year")

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


  cohortsSummary <- cohortInfoTable |> dplyr::collect() |>
    dplyr::left_join(histogramCohortStartYear, by = "cohort_definition_id") |>
    dplyr::left_join(histogramCohortEndYear, by = "cohort_definition_id") |>
    dplyr::left_join(sexCounts, by = "cohort_definition_id")


  return(cohortsSummary)
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
#' @noRd
#'
#' @export
#'
deleteCohortFromCohortTable  <- function(database_settings, cohort_names){

  # get settings
  connectionDetails  <-
    schema             <-
    cohort_table_name  <- database_settings$tables$cohort_table
  cohort_names_table_name <- paste0(cohort_table_name, "_names")

  # Connect
  connection <- DatabaseConnector::connect(database_settings$connectionDetails)

  # Function
  sql <- SqlRender::readSql(system.file("sql/sql_server/DeleteCohortFromCohortTables.sql", package = "HadesExtras", mustWork = TRUE))
  sql <- SqlRender::render(
    sql = sql,
    cohort_database_schema = database_settings$schemas$scratch,
    cohort_table = database_settings$tables$cohort_table,
    cohort_name_table = database_settings$tables$cohort_table,
    cohort_names_to_delete = paste0("(", paste(paste0("'",cohort_names,"'"), collapse = ", "), ")"),
    warnOnMissingParameters = TRUE
  )
  sql <- SqlRender::translate(
    sql = sql,
    targetDialect = connection@dbms
  )
  DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)

  # Disconect and return
  DatabaseConnector::disconnect(connection)

  return(TRUE)
}


