#' Create an empty cohorts summary
#'
#' @return A tibble with cohort summary structure
#' @importFrom tibble tibble
#' @export
#'
createEmptyCohortsSummary <- function() {

  emptyCohortsSummary <- tibble::tibble(
    databaseName = as.character(NA),
    cohortId = as.double(NA),
    shortName = as.character(NA),
    cohortName = as.character(NA),
    cohortEntries = as.integer(NA),
    cohortSubjects = as.integer(NA),
    histogramCohortStartYear = list(
      tibble::tibble(
        year = as.integer(NA),
        n = as.integer(NA),
        .rows = 0)
      ),
    histogramCohortEndYear = list(
      tibble::tibble(
        year = as.integer(NA),
        n = as.integer(NA),
        .rows = 0)
      ),
    histogramBirthYear = list(
      tibble::tibble(
        year = as.integer(NA),
        n = as.integer(NA),
        .rows = 0)
    ),
    sexCounts = list(
      tibble::tibble(
        sex = as.character(NA),
        n = as.integer(NA),
        .rows = 0)),
    buildInfo = list(
      LogTibble$new()
    ),
    .rows = 0 )

  return(emptyCohortsSummary)
}

#' Set NA to 0 in a cohortsSummary, and tibles to be empty
#'
#' @param cohortsSummary tibble in cohortsSummary format
#'
#' @return A tibble with cohort summary structure
#' @importFrom tibble tibble
#' @importFrom dplyr mutate if_else
#' @export
#'
correctEmptyCohortsInCohortsSummary <- function(cohortsSummary) {

  emptyHistogram = list(
    tibble::tibble(
      year = as.integer(NA),
      n = as.integer(NA),
      .rows = 0)
  )
  emptysexCounts = list(
    tibble::tibble(
      sex = as.character(NA),
      n = as.integer(NA),
      .rows = 0))

  log <- LogTibble$new()
  log$ERROR("", "Cohort is empty")

  cohortsSummary <- cohortsSummary |>
    dplyr::mutate(
      histogramCohortStartYear = dplyr::if_else(is.na(cohortEntries), emptyHistogram, histogramCohortStartYear),
      histogramCohortEndYear = dplyr::if_else(is.na(cohortEntries), emptyHistogram, histogramCohortEndYear),
      histogramBirthYear = dplyr::if_else(is.na(cohortEntries), emptyHistogram, histogramBirthYear),
      sexCounts = dplyr::if_else(is.na(cohortEntries), emptysexCounts, sexCounts),
      buildInfo = dplyr::if_else(is.na(cohortEntries), list(log$clone()), buildInfo),
      cohortEntries = dplyr::if_else(is.na(cohortEntries), 0L, as.integer(cohortEntries)),
      cohortSubjects = dplyr::if_else(is.na(cohortSubjects), 0L, as.integer(cohortSubjects))
    ) |>
    dplyr::select(
      databaseName,  cohortId, cohortName,  shortName,
      cohortEntries, cohortSubjects,
      histogramCohortStartYear, histogramCohortEndYear, histogramBirthYear, sexCounts,
      buildInfo
    )

  return(cohortsSummary)
}


#' Check if a tibble is of CohortsSummary format.
#'
#' This function performs various validations on the CohortsSummary tibble to ensure its integrity and correctness. The validations include:
#'
#' - Checking if CohortsSummary is a tibble.
#' - Checking if the CohortsSummary tibble contains the required column names
#' - Validating the types
#'
#' @param tibble The tibble to be checked
#'
#' @return TRUE if the tibble is of CohortsSummary format, an array of strings with the failed checks
#' @export
#'
checkCohortsSummary  <- function(tibble) {
  collection <- .assertCollectionCohortsSummary(tibble)
  if (collection$isEmpty()) {
    return(TRUE)
  } else {
    return(collection$getMessages())
  }
}


#' @export
#' @importFrom checkmate reportAssertions
#' @rdname checkCohortsSummary
assertCohortsSummary  <- function(tibble) {
  collection <- .assertCollectionCohortsSummary(tibble)
  if (!collection$isEmpty()) {
    checkmate::reportAssertions(collection)
  }
}

#' Internal function to validate cohort data
#'
#' @param CohortsSummary The cohort data tibble to be validated
#'
#' @importFrom checkmate assertTibble
#' @importFrom validate check_that
#' @importFrom dplyr filter mutate if_else pull
#' @importFrom purrr map
#' @importFrom stringr str_replace_all
.assertCollectionCohortsSummary <- function(CohortsSummary) {

  collection = checkmate::makeAssertCollection()

  CohortsSummary |> checkmate::assertTibble()
  # check column names
  missingCollumnNames <- setdiff(
    createEmptyCohortsSummary() |>  names(),
    CohortsSummary |> names()
  )
  if(length(missingCollumnNames)){
    paste("Table is missing the following columns: ", paste0(missingCollumnNames, collapse = ", ")) |>
      collection$push()
  }
  # validate
  failsNames <- CohortsSummary |> validate::check_that(
    # type
    cohortName.is.not.of.type.character = is.character(cohortName),
    cohortId.is.not.of.type.double = is.double(cohortId),
    sourceCohortId.is.not.of.type.double = is.double(sourceCohortId),
    cohortDescription.is.not.of.type.character = is.character(cohortDescription),
    cohortEntries.is.not.of.type.integer = is.integer(cohortEntries),
    cohortSubjects.is.not.of.type.integer = is.integer(cohortSubjects),
    histogram_cohort_start_year.is.not.of.type.list = is.list(histogram_cohort_start_year),
    histogram_cohort_end_year.is.not.of.type.list  = is.list(histogram_cohort_end_year),
    count_sex.is.not.of.type.list = is.list(count_sex)
    # missing
    # TODO
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

