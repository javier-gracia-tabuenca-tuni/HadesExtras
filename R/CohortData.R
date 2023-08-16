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



cohortDataToCohortDefinitionSet <- function(
    cohortData,
    cohortIdOffset = 0L,
    skipCohortDataCheck = FALSE
    ){

  #
  # Validate parameters
  #
  checkmate::assertInt(cohortIdOffset)
  checkmate::assertLogical(skipCohortDataCheck)

  if(skipCohortDataCheck == TRUE){
    assertCohortData(cohortData)
  }

  #
  # Function
  #
  sqlToRender <- SqlRender::readSql(system.file("sql/sql_server/ImportCohortTable.sql", package = "HadesExtras", mustWork = TRUE))

  cohortDefinitionSet <- cohortData |>
    tidyr::nest(.key = "cohort", .by = c("cohort_name")) |>
    dplyr::transmute(
      cohortId = dplyr::row_number()+cohortIdOffset,
      cohortName = cohort_name,
      json = purrr::map_chr(.x = cohort, .f=.cohortDataToJson),
      sql = purrr::map2_chr(
        .x = cohortId,
        .y = cohort,
        .f=~{paste0("--",  digest::digest(.y), "\n",
                   SqlRender::render(
                     sql = sqlToRender,
                     source_cohort_table = getOption("cohortDataImportTmpTableName", "tmp_cohortdata"),
                     source_cohort_id = .x,
                     is_temp_table = TRUE
                   ))}
      )
    )

  return(cohortDefinitionSet)
}


.cohortDataToJson <- function(cohortData){
  RJSONIO::toJSON(
    list(
      cohortType = "FromCohortData",
      cohortData = cohortData
    )
  )
}

.jsonToCohortData <- function(cohortDefinitionSet) {

  cohortData <- cohortDefinitionSet |>
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

}










