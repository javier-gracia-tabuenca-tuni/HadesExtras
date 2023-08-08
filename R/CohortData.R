#' Create an empty cohort data
#'
#' @return A tibble with cohort data structure
#' @importFrom tibble tibble
#' @export
#'
createEmptyCohortData <- function() {
  empty_cohortData <- tibble::tibble(
    cohort_name = as.character(NA),
    person_source_value = as.character(NA),
    cohort_start_date = as.Date(NA),
    cohort_end_date = as.Date(NA),
    .rows = 0
  )
  return(empty_cohortData)
}

#' readCohortData
#'
#' Reads a cohort data file and returns a data frame.
#'
#' @param pathCohortDataFile The path to the cohort data file.
#' @param delim The delimiter used in the cohort data file.
#'
#' @return A data frame containing the cohort data.
#'
#' @importFrom readr cols col_character col_date read_delim
#' @importFrom checkmate assertFile
#'
#' @export
readCohortData <- function(pathCohortDataFile, delim = "," ){

  checkmate::assertFile(pathCohortDataFile)

  col_types <- readr::cols(
    cohort_name = readr::col_character(),
    person_source_value = readr::col_character(),
    cohort_start_date = readr::col_date(format = ""),
    cohort_end_date = readr::col_date(format = "")
  )

  cohortData <- readr::read_delim(
    pathCohortDataFile,
    delim = delim,
    col_types = col_types )

  return(cohortData)
}

#' Check if a tibble is of cohortData format.
#'
#' This function performs various validations on the cohortData tibble to ensure its integrity and correctness. The validations include:
#'
#' - Checking if cohortData is a tibble.
#' - Checking if the cohortData tibble contains the required column names: cohort_name, person_source_value, cohort_start_date, cohort_end_date.
#' - Validating the types of cohort_name, person_source_value, cohort_start_date, and cohort_end_date columns (character and Date types, respectively).
#' - Checking for missing values in cohort_name and person_source_value columns.
#' - Verifying the order of cohort_start_date and cohort_end_date, ensuring cohort_start_date is older than cohort_end_date.
#'
#' @param tibble The tibble to be checked
#'
#' @return TRUE if the tibble is of cohortData format, an array of strings with the failed checks
#' @export
#'
checkCohortData  <- function(tibble) {
  collection <- .assertCollectionCohortData(tibble)
  if (collection$isEmpty()) {
    return(TRUE)
  } else {
    return(collection$getMessages())
  }
}


#' @export
#' @importFrom checkmate reportAssertions
#' @rdname checkCohortData
assertCohortData  <- function(tibble) {
  collection <- .assertCollectionCohortData(tibble)
  if (!collection$isEmpty()) {
    checkmate::reportAssertions(collection)
  }
}

#' Internal function to validate cohort data
#'
#' @param cohortData The cohort data tibble to be validated
#'
#' @importFrom checkmate assertTibble
#' @importFrom validate check_that
#' @importFrom dplyr filter mutate if_else pull
#' @importFrom purrr map
#' @importFrom stringr str_replace_all
.assertCollectionCohortData <- function(cohortData) {

  collection = checkmate::makeAssertCollection()

  cohortData |> checkmate::assertTibble()
  # check column names
  missingCollumnNames <- setdiff(
    c("cohort_name", "person_source_value", "cohort_start_date", "cohort_end_date"),
    cohortData |> names()
  )
  if(length(missingCollumnNames)){
    paste("Table is missing the following columns: ", paste0(missingCollumnNames, collapse = ", ")) |>
      collection$push()
  }
  # validate
  failsNames <- cohortData |> validate::check_that(
    # type
    cohort_name.is.not.of.type.character = is.character(cohort_name),
    person_source_value.is.not.of.type.character = is.character(person_source_value),
    cohort_start_date.is.not.of.type.date = class(cohort_start_date)=="Date",
    cohort_end_date.is.not.of.type.date = class(cohort_end_date)=="Date",
    # missing
    rows.are.missing.cohort_name = !is.na(cohort_name),
    rows.are.missing.person_source_value = !is.na(person_source_value),
    # order
    rows.have.cohort_start_date.older.than.cohort_end_date = cohort_start_date < cohort_end_date
  ) |>
    validate::summary() |>
    dplyr::filter(fails!=0) |>
    dplyr::mutate(msg = dplyr::if_else(items > 1, as.character(fails), "")) |>
    dplyr::mutate(msg = paste(msg, stringr::str_replace_all(name ,"\\.", " "))) |>
    dplyr::pull(msg)

  # add to collection
  failsNames |> purrr::map(.f=~collection$push(.x))

  return(collection)

}



#' cohortDataToCohortDefinitionSet
#'
#' Generates a cohortDefinitionSet based on cohortData tibble.
#' Finds the person_id based on the person_source_value in cohortData.
#' If cohort_start_date and cohort_end_date is missing in cohortData,
#' it sets the first observation_period_start_date and last observation_period_end_date respectvively. #'
#' The cohortDefinitionSet includes a sql to copy from a temporary table with the pre-processed cohortData and
#' a tibble with information on the pre-processing.
#'
#'
#' @param connectionDetails The connection details to the database. If not connection is provided.
#' @param connection The database connection. If not connectionDetails is provided.
#' @param cdmDatabaseSchema The schema of the CDM database.
#' @param cohortData The cohort data.
#'
#' @importFrom DatabaseConnector connect disconnect
#' @importFrom dplyr select distinct mutate left_join filter compute
#' @importFrom tidyr nest
#' @importFrom SqlRender readSql render
#' @importFrom dbplyr remote_name
#' @importFrom purrr map_chr
#'
#' @return The cohortDefinitionSet with additional column
#'
#' @export
#'
cohortDataToCohortDefinitionSet <- function(
    connectionDetails = NULL,
    connection = NULL,
    cdmDatabaseSchema,
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

  #
  # Function
  #

  # replace cohortName with cohortId
  cohortDataNameToId <- cohortData |>
    dplyr::select(cohort_name) |>
    dplyr::distinct() |>
    dplyr::mutate(cohort_definition_id = dplyr::row_number())

  cohortData <- cohortData |>
    dplyr::left_join(cohortDataNameToId, by = "cohort_name") |>
    dplyr::select(-cohort_name)


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
    dplyr::select(cohort_definition_id, subject_id=person_id, cohort_start_date, cohort_end_date) |>
    dplyr::compute()

  # create cohortDefinitionSet
  addCohortIdsToSql <- function(cohort_definition_ids){

  }
  sqlToRender <- SqlRender::readSql(system.file("sql/sql_server/ImportCohortTable.sql", package = "HadesExtras", mustWork = TRUE))
  tableNameToRender <- dbplyr::remote_name(toAppend)

  cohortDefinitionSet <- cohortDataNameToId |>
    dplyr::left_join(checkOnCohortData, by = "cohort_definition_id") |>
    dplyr::mutate(
      sql = purrr::map_chr(
        .x = cohort_definition_id,
        .f=~SqlRender::render(
          sql = sqlToRender,
          source_cohort_table = tableNameToRender,
          source_cohort_id = .x,
          is_temp_table = TRUE
        )),
      json = ""
    ) |>
    tidyr::nest(.key = "extra_info", .by = c("cohort_name", "cohort_definition_id", "sql", "json")) |>
    dplyr::rename(cohortName = cohort_name, cohortId = cohort_definition_id)

  return(cohortDefinitionSet)
}




.cohortDataToSQLcomment <- function(cohortData, sql){

  cohortDataSqlComment <- paste0(
    "-- START COHORTDATA\n",
    "--", cohortData |>  RJSONIO::toJSON() |> stringr::str_replace_all("\\n", "\n--" ),
    "\n-- END COHORTDATA\n"
    )

  sql <- paste0(cohortDataSqlComment, sql)

  return(sql)

}


.cohortDataFromSQLcomment <- function(sqlWithCohortData){

  cohortData <-  sqlWithCohortData |>
    stringr::str_extract("(?<=-- START COHORTDATA)(.|\\n)*(?=\\n-- END COHORTDATA)") |>
  stringr::str_remove_all("\\n--") |>
    RJSONIO::fromJSON() |>
    tibble::as_tibble() |>
    dplyr::mutate(
      cohort_start_date = as.Date(cohort_start_date),
      cohort_end_date = as.Date(cohort_end_date)
    )

  sql <- sqlWithCohortData |>
    stringr::str_remove("(-- START COHORTDATA)(.|\\n)*(-- END COHORTDATA)")

  return(list(
    cohortData = cohortData,
    sql = sql
    ))

}


















