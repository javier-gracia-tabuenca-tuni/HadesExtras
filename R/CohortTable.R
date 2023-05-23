
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

createCohortTables <- function(database_settings) {

  # connect
  connection <- DatabaseConnector::connect(database_settings$connectionDetails)

  sql <- SqlRender::readSql(system.file("sql/sql_server/CreateCohortTables.sql", package = "HadesExtras", mustWork = TRUE))
  sql <- SqlRender::render(
    sql = sql,
    cohort_database_schema = database_settings$schemas$scratch,
    cohort_table = database_settings$tables$cohort_table,
    cohort_name_table = database_settings$tables$cohort_names_table,
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



#' Append cohort data to cohort table
#'
#' This function appends cohort data to the cohort table in the database.
#'
#' @param database_settings A list containing the necessary settings for connecting to the database.
#' @param cohort_data A data frame containing cohort data.
#'
#' @importFrom DatabaseConnector connect disconnect
#' @importFrom dplyr tbl copy_to distinct mutate pull rows_append slice
#'
#' @return A data frame containing cohort data.
#'
#' @export
#'


appendCohortDataToCohortTable <- function(database_settings, cohort_data){

  ###
  ## validate input parameters
  ###
  cohort_table_cohort_names <- getCohortNamesFromCohortTable(database_settings)
  cohort_data_cohort_names <- cohort_data |> dplyr::distinct(cohort_name) |> dplyr::pull()

  names_exists_in_cohort_table <- intersect(cohort_table_cohort_names, cohort_data_cohort_names)
  if(length(names_exists_in_cohort_table)!=0){stop("Cohorts already exists with these names ", paste(names_exists_in_cohort_table, collapse = ", "))}


  # create log
  log <- logTibble_NewLog()

  # Connect, collect tables, upload tables
  connection <- DatabaseConnector::connect(database_settings$connectionDetails)

  cohort_table <- dplyr::tbl(connection, tmp_inDatabaseSchema(database_settings$schemas$scratch, database_settings$tables$cohort_table))
  cohort_names_table <- dplyr::tbl(connection, tmp_inDatabaseSchema(database_settings$schemas$scratch, database_settings$tables$cohort_names_table))

  person_table <- dplyr::tbl(connection, tmp_inDatabaseSchema(database_settings$schemas$CDM, "person"))
  observation_period_table <- dplyr::tbl(connection, tmp_inDatabaseSchema(database_settings$schemas$CDM, "observation_period"))

  # get safely max cohort_definition_id in cohort_table
  max_cohort_id_in_cohort_table <- cohort_names_table |>
    dplyr::pull(cohort_definition_id) |> max()
  max_cohort_id_in_cohort_table <- ifelse(is.na(max_cohort_id_in_cohort_table), 0, max_cohort_id_in_cohort_table)

  # add a new cohort_definition_id to each cohort name in cohort_data
  cohort_data <- cohort_data |>
    tidyr::nest(.by = "cohort_name") |>
    dplyr::mutate(cohort_definition_id = max_cohort_id_in_cohort_table + dplyr::row_number()) |>
    tidyr::unnest(data)

  # copy table to database
  cohort_data_table <- dplyr::copy_to(connection, cohort_data)

  # join to cohort_data_table cohort_names_table.cohort_name; person.person_id; observation_period period dates
  to_append_cohort_table <- cohort_data_table |>
    dplyr::inner_join(
      person_table |> dplyr::select(person_id, person_source_value),
      by = "person_source_value"
    ) |>
    dplyr::left_join(
      observation_period_table |>
        dplyr::select(person_id, observation_period_start_date, observation_period_end_date) |>
        dplyr::group_by(person_id) |>
        dplyr::summarise(
          observation_period_start_date = min(observation_period_start_date, na.rm = TRUE),
          observation_period_end_date = max(observation_period_end_date, na.rm = TRUE)
        ),
      by = "person_id"
    )

  # log checks
  message_added_n_patients_per_cohort_name <-
    cohort_data_table |> dplyr::count(cohort_name, name="n_cohort_data") |>
    dplyr::left_join(
      to_append_cohort_table |> dplyr::count(cohort_name, name="n_cohort_table")
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

  log <- logTibble_WARNING(log, "source_person_id not found in database", message_added_n_patients_per_cohort_name)

  message_cohort_start_date_na_per_cohort_name  <- to_append_cohort_table  |>
    dplyr::count(cohort_name, is_na=is.na(cohort_start_date)) |>
    dplyr::filter(is_na!=0) |>
    dplyr::collect() |>
    dplyr::mutate(n_cohort_start_date_na = paste0("For cohort '", cohort_name, "' ",n, " cohort_start_dates are NA and were replaced by observation_period_start_date")) |>
    dplyr::pull(n_cohort_start_date_na) |> as.character()

  log <- logTibble_WARNING(log, "Filled in cohort_start_date", message_cohort_start_date_na_per_cohort_name)

  message_cohort_end_date_na_per_cohort_name  <- to_append_cohort_table  |>
    dplyr::count(cohort_name, is_na=is.na(cohort_end_date)) |>
    dplyr::filter(is_na!=0) |>
    dplyr::collect() |>
    dplyr::mutate(n_cohort_end_date_na = paste0("For cohort '", cohort_name, "' ",n, " cohort_end_dates are NA and were replaced by observation_period_end_date")) |>
    dplyr::pull(n_cohort_end_date_na) |> as.character()

  # append to cohort_table
  dplyr::rows_append(
    cohort_table,
    to_append_cohort_table |>
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
    cohort_names_table,
    to_append_cohort_table |>
      dplyr::distinct(cohort_definition_id, cohort_name) |>
      dplyr::mutate(atlas_cohort_definition_id = as.integer(NA)),
    in_place = TRUE
  )

  log <- logTibble_WARNING(log, "Filled in cohort_end_date", message_cohort_end_date_na_per_cohort_name)


  if(nrow(log)==0){
    message_added_person_per_cohort <- cohort_data  |>
    dplyr::count(cohort_name)  |>
    dplyr::mutate(n_patients = paste0("For cohort '", cohort_name, "' ", n, " patients were added to the cohort table"))  |>
    dplyr::pull(n_patients)

    log <- logTibble_INFO(log, "Cohort table filled in successfully", message_added_person_per_cohort)
  }

  # drop temp tables and disconnect
  DatabaseConnector::dropEmulatedTempTables(connection)
  DatabaseConnector::disconnect(connection)

  return(log)
}


#' Get cohort names from cohort table
#'
#' This function retrieves unique cohort names from a cohort table in the database.
#'
#' @param connectionDetails A list containing the necessary details to establish a database connection.
#' @param schema The name of the schema where the cohort table is located.
#' @param cohort_table_name The name of the cohort table.
#'
#' @importFrom DatabaseConnector connect
#' @importFrom dplyr tbl pull
#'
#' @return A character vector of cohort names.
#'
#' @noRd
#'
#' @export
#'
getCohortNamesFromCohortTable   <- function(database_settings){

  # Connect and collect tables
  connection <- DatabaseConnector::connect(database_settings$connectionDetails)

  cohort_names_table <- dplyr::tbl(connection, tmp_inDatabaseSchema(database_settings$schemas$scratch, database_settings$tables$cohort_names_table))

  # Function
  cohort_names  <- cohort_names_table |>
    dplyr::pull(cohort_name)

  # Disconect and return
  DatabaseConnector::disconnect(connection)

  return(cohort_names)
}

#' Get cohort table summary
#'
#' This function retrieves a summary of the cohort tables,
#' Summary per each cohort the number of entries, number of unique persons, number of entries per start year, and number of entries per end year
#'
#' @param connectionDetails A list containing the necessary details to establish a database connection.
#' @param schema The name of the schema where the cohort table and cohort names table are located.
#' @param cohort_table_name The name of the cohort table.
#'
#' @importFrom DatabaseConnector connect year
#' @importFrom dplyr tbl left_join mutate group_by summarise n n_distinct collect mutate_if
#'
#' @return A data frame with the summary information of the cohort table.
#'
#' @noRd
#'
#' @export
#'
getCohortTableSummary  <- function(database_settings){

    # get settings
    connectionDetails  <- database_settings$connectionDetails
    scratch_schema     <- database_settings$schemas$scratch
    cohort_table_name  <- database_settings$tables$cohort_table
    cohort_names_table_name <- paste0(cohort_table_name, "_names")

    # connect to database and tables
    connection <- DatabaseConnector::connect(connectionDetails)

    cohort_table <- dplyr::tbl(connection, tmp_inDatabaseSchema(scratch_schema, cohort_table_name))
    cohort_names_table <- dplyr::tbl(connection, tmp_inDatabaseSchema(scratch_schema, cohort_names_table_name))
    person_table  <- dplyr::tbl(connection, tmp_inDatabaseSchema(database_settings$schemas$CDM, "person"))
    concept_table  <- dplyr::tbl(connection, tmp_inDatabaseSchema(database_settings$schemas$vocab, "concept"))

    # function
    histogram_cohort_start_year <- cohort_table |>
      dplyr::mutate( cohort_start_year = year(cohort_start_date) ) |>
      dplyr::count(cohort_definition_id, cohort_start_year)  |>
      dplyr::collect() |>
      dplyr::nest_by(cohort_definition_id, .key = "histogram_cohort_start_year")

    histogram_cohort_end_year <- cohort_table |>
      dplyr::mutate( cohort_end_year = year(cohort_end_date) ) |>
      dplyr::count(cohort_definition_id, cohort_end_year)  |>
      dplyr::collect() |>
      dplyr::nest_by(cohort_definition_id, .key = "histogram_cohort_end_year")

    sex_counts <- cohort_table  |>
    dplyr::left_join(
      person_table  |> dplyr::select(person_id, gender_concept_id),
      by = c("subject_id" = "person_id")
      ) |>
      dplyr::left_join(
        concept_table |> dplyr::select(concept_id, concept_name),
        by = c("gender_concept_id" = "concept_id")
      ) |>
      dplyr::count(cohort_definition_id, sex=concept_name)|>
      dplyr::collect() |>
      dplyr::nest_by(cohort_definition_id, .key = "count_sex")


    cohorts_summary <- cohort_names_table |> dplyr::collect() |>
      dplyr::left_join(histogram_cohort_start_year, by = "cohort_definition_id") |>
      dplyr::left_join(histogram_cohort_end_year, by = "cohort_definition_id") |>
      dplyr::left_join(sex_counts, by = "cohort_definition_id")

    # Disconnect and return
    DatabaseConnector::disconnect(connection)

    return(cohorts_summary)
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


